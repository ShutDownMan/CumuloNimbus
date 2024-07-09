use anyhow::{bail, anyhow, Result};
use flate2::write::ZlibEncoder;
use flate2::read::ZlibDecoder;
use flate2::Compression;
use futures_lite::stream::StreamExt;
use lapin::{
    message::Delivery, options::*, publisher_confirm::Confirmation, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties, Consumer
};
use std::{io::prelude::*, sync::{Arc, Mutex}};
use tracing::{debug, error, info, warn};
use std::collections::HashMap;
use capnp::message::ReaderOptions;

extern crate num;

pub mod schemas;

pub use capnp::serialize_packed;
pub use capnp::traits::HasTypeId;


#[derive(Debug, thiserror::Error)]
pub struct CallbackError;

impl std::fmt::Display for CallbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Error invoking callback")
    }
}

#[derive(Debug)]
pub enum MessagePriority {
    Alpha = 2,
    Beta = 1,
    Omega = 0,
}

pub type CapnpBuilder<A> = capnp::message::Builder<A>;
pub type CapnpReader = capnp::message::Reader<capnp::serialize::OwnedSegments>;
pub type LapinAMQPProperties = lapin::protocol::basic::AMQPProperties;

type SubscriberType = Arc<Mutex<HashMap<String, HashMap<u64, MessageHandlerCallbackType>>>>;

type MessageHandlerCallbackType = Box<dyn Fn(CapnpReader, LapinAMQPProperties) -> Result<()> + Send + 'static>;

pub struct ServiceBus {
    _connection: Connection,
    channel: Channel,
    subscriptions: SubscriberType
}

impl ServiceBus {
    pub async fn new() -> Result<Self> {
        let addr = std::env::var("SERVICE_BUS_ADDRESS")
            .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());

        let connection_properties = ConnectionProperties::default();

        let connection = Connection::connect(&addr, connection_properties).await?;

        info!("CONNECTED");

        let channel = connection.create_channel().await?;
        channel.basic_qos(1, BasicQosOptions::default()).await?;

        let subscriptions = Arc::new(Mutex::new(HashMap::new()));

        Ok(ServiceBus {
            _connection: connection,
            channel,
            subscriptions,
        })
    }

    pub async fn simple_queue_declare(&self, queue_name: &str, routing_key: &str) -> Result<()> {
        info!("declaring queue {}", queue_name);
        let dead_letter_queue = format!("{}.dead-letter", queue_name);
        let dead_letter_routing_key = format!("{}.dead-letter", routing_key);

        info!("declaring dead letter queue {}", dead_letter_queue);
        self.channel
            .queue_declare(
                dead_letter_queue.as_str(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!("binding dead letter queue to exchange");
        self.channel
            .queue_bind(
                dead_letter_queue.as_str(),
                "amq.direct",
                dead_letter_routing_key.as_str(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        info!("Bound dead letter queue");

        let mut fields = FieldTable::default();
        fields.insert("x-max-priority".into(), 2.into());
        fields.insert("x-dead-letter-exchange".into(), lapin::types::AMQPValue::LongString("amq.direct".into()));
        fields.insert("x-dead-letter-routing-key".into(), lapin::types::AMQPValue::LongString(dead_letter_routing_key.into()));

        let queue_declare_options = QueueDeclareOptions {
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            passive: false,
        };

        self.channel
            .queue_declare(
                queue_name,
                queue_declare_options,
                fields,
            )
            .await?;
        info!("Declared queue");

        info!("binding queue {} to exchange {}", queue_name, routing_key);
        self.channel
            .queue_bind(
                queue_name,
                "amq.direct",
                routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        info!("Bound queue");

        Ok(())
    }

    pub async fn basic_publish_message(&self, queue_name: &str, message: &[u8]) -> Result<()> {
        info!("publishing message to queue {}", queue_name);

        let confirm = self
            .channel
            .basic_publish(
                "",
                queue_name,
                BasicPublishOptions::default(),
                message,
                BasicProperties::default(),
            )
            .await?
            .await?;

        assert_eq!(confirm, Confirmation::NotRequested);

        Ok(())
    }

    pub async fn persistent_publish_message(&self, queue_name: &str, message: &[u8]) -> Result<()> {
        info!("publishing message to queue {}", queue_name);

        let confirm = self
            .channel
            .basic_publish(
                "",
                queue_name,
                BasicPublishOptions::default(),
                message,
                BasicProperties::default()
                    .with_delivery_mode(2)
                    .with_priority(0),
            )
            .await?
            .await?;

        assert_eq!(confirm, Confirmation::NotRequested);

        Ok(())
    }

    // publish message with capnp
    pub async fn intercom_publish(
        &self,
        exchange: &str,
        routing_key: &str,
        message_type_id: u64,
        message: &[u8],
        compress: bool,
        priority: Option<MessagePriority>,
    ) -> Result<()> {
        info!("publishing message to queue {}", routing_key);

        debug!("raw message length: {}", message.len());
        let mut properties = BasicProperties::default();
        let mut headers = FieldTable::default();

        headers.insert("message-type".into(), message_type_id.into());

        properties = properties.with_headers(headers)
            .with_content_type("application/capnp".into());

        let message = if compress {
            properties = properties.with_content_encoding("gzip".into());
            gzip_encode(message).await?
        } else {
            message.to_vec()
        };

        if compress {
            debug!("compressed message length: {}", message.len());
        } else {
            debug!("uncompressed message length: {}", message.len());
        }

        properties = properties
            .with_priority(priority.unwrap_or(MessagePriority::Omega) as u8)
            .with_delivery_mode(2);

        let _confirm = self
            .channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                &message,
                properties,
            )
            .await?;

        Ok(())
    }

    pub fn subscribe(&self, topic: &str, message_type_id: u64, callback: MessageHandlerCallbackType) -> Result<()>
    {
        let m_topic: String = topic.to_string();
        let m_subscriptions = self.subscriptions.clone();
        let result = std::thread::spawn(move || {
            let mut subs = m_subscriptions.lock().unwrap();

            // check if exists in hashmap
            if let Some(existing_subs) = subs.get_mut(&m_topic) {
                // if exists, insert into the existing hashmap
                existing_subs.insert(message_type_id, callback);
            } else {
                // if does not exist, create new entry
                subs.insert(m_topic.clone(), HashMap::from([(message_type_id, callback)]));
            }
        }).join();

        match result {
            Ok(_) => Ok(()),
            Err(_) => bail!("Could not subscribe to topic {:?}", topic)
        }
    }

    pub async fn listen_forever(&self) -> Result<()> {
        let channel = self.channel.clone();
        let subscriptions = self.subscriptions.clone();

        let consumers = create_consumers(&channel, &subscriptions).await;

        handle_consumers(consumers, subscriptions).await;

        Ok(())
    }

}

async fn create_consumers(channel: &Channel, subscriptions: &SubscriberType) -> Vec<Consumer> {
    let subs = subscriptions.lock().unwrap();

    let mut consumers = Vec::new();
    for topic in subs.keys() {
        debug!("Creating consumer for {}", topic);
        let consumer_future = channel.basic_consume(
            topic,
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        );
        consumers.push(consumer_future);
    }

    futures::future::join_all(consumers).await.into_iter()
        .map(|result| result.unwrap())
        .collect()
}

async fn handle_consumers(consumers: Vec<Consumer>, subscriptions: SubscriberType) {
    let tasks = consumers.into_iter().map(|mut consumer| {
        let inner_subscriptions = subscriptions.clone();

        async_global_executor::spawn(async move {
            let topic = consumer.queue().to_string();
            debug!("listening for messages in topic {}", topic);

            while let Some(delivery) = consumer.next().await {
                match delivery {
                    Ok(delivery) => {
                        process_message(delivery, inner_subscriptions.clone(), &topic).await;
                    },
                    Err(e) => {
                        error!("error in consumer: {:?}", e);
                    }
                }
            }
        })
    }).collect::<Vec<async_global_executor::Task<()>>>();

    futures::future::join_all(tasks).await;
}


async fn process_message(
    delivery: Delivery,
    subscriptions: SubscriberType,
    topic: &str,
) {

    let properties = delivery.properties.clone();
    let dbg_headers = properties.clone().headers().clone().map(|h| format!("{:?}", h)).unwrap_or("No headers".to_string());
    let dbg_content_type = properties.content_type().clone().map(|ct| format!("{:?}", ct)).unwrap_or("No content type".to_string());
    let dbg_delivery_mode = properties.delivery_mode().map(|dm| format!("{:?}", dm)).unwrap_or("No delivery mode".to_string());
    let dbg_content_encoding = properties.content_encoding().clone().map(|ce| format!("{:?}", ce)).unwrap_or("No content encoding".to_string());
    let dbg_message_id = properties.message_id().clone().map(|id| format!("{:?}", id)).unwrap_or("No message id".to_string());
    let dbg_timestamp = properties.timestamp().map(|ts| format!("{:?}", ts)).unwrap_or("No timestamp".to_string());
    let dbg_body_length = delivery.data.len();

    info!("Received message in topic {} | Message ID: {} | Length: {}", topic, dbg_message_id, dbg_body_length);
    debug!("Message Headers: {}", &dbg_headers);
    debug!("Content Type: {}", &dbg_content_type);
    debug!("Delivery Mode: {}", &dbg_delivery_mode);
    debug!("Content Encoding: {}", &dbg_content_encoding);
    debug!("Message ID: {}", &dbg_message_id);
    debug!("Timestamp: {}", &dbg_timestamp);
    debug!("Message Body Length: {}", &dbg_body_length);

    let mut message_type = None;

    let headers = delivery.properties.headers().clone().unwrap();

    for header in headers.into_iter() {
        let key = header.0.as_str();
        let new_header = (key, header.1);

        match new_header {
            // timestamp is being used because it is a u64
            ("message-type", value) => message_type = value.as_timestamp(),
            _ => ()
        }
    }

    let dbg_message_type = message_type.map(|mt| format!("{:?}", mt)).unwrap_or("No message type".to_string());

    debug!("Message Type: {}", &dbg_message_type);

    // check if message is really compressed before decoding
    let data_ref = &delivery.data;
    let body = match properties.content_encoding() {
        Some(encoding) => {
            if encoding.as_str() == "gzip" {
                gzip_decode(data_ref).await.ok()
            } else {
                Some(data_ref.clone())
            }
        },
        None => Some(data_ref.clone())
    };
    if let Some(body) = body {
        let metadata = properties.clone();
        let topic_callback = topic.to_string();
        debug!("Handling message");
        let _callback_result = std::thread::spawn(move || {
            let body_slice = &mut &body[..];
            handle_subscription_callback(subscriptions.clone(), topic_callback, message_type, body_slice, metadata)
        }).join();

        match _callback_result {
            Ok(Ok(_)) => {
                debug!("Message handled successfully");

                debug!("Acking message");
                delivery.ack(BasicAckOptions::default()).await.expect("ack");
                debug!("Message acked");

            },
            Ok(Err(e)) => {
                match e.downcast_ref::<CallbackError>() {
                    Some(_) => {
                        warn!("Callback error, sending message to dead letter queue");
                        let _ = delivery.reject(BasicRejectOptions { requeue: false }).await;
                    },
                    None => {
                        error!("Error handling message: {:?}", e);
                    }
                }
            },
            Err(e) => {
                error!("Error with message handler task: {:?}", e);

                warn!("Sending message to dead letter queue");
                let _ = delivery.reject(BasicRejectOptions { requeue: false }).await;
            }
        }
    }
}

fn handle_subscription_callback(
    subscriptions: SubscriberType,
    topic: String,
    message_type_id: Option<u64>,
    body: &mut &[u8],
    metadata: LapinAMQPProperties
) -> Result<()> {
    debug!("Locking subscriptions");

    let locked_subs = subscriptions.lock().map_err(|_| anyhow!("Failed to lock subscriptions"))?;
    let current_topic_subs = locked_subs.get(&topic).ok_or_else(|| anyhow!("Topic not found"))?;

    debug!("Handling message");

    let message_type = message_type_id.and_then(num::FromPrimitive::from_u64).ok_or_else(|| anyhow!("Invalid message type"))?;
    let callback = current_topic_subs.get(&message_type).ok_or_else(|| anyhow!("Message type not found"))?;

    debug!("Reading message");

    let reader = capnp::serialize_packed::read_message(body, ReaderOptions::new())?;

    debug!("Invoking callback");
    let result = callback(reader, metadata);

    debug!("Callback result: {:?}", result);

    if let Err(e) = result {
        error!("Error invoking callback: {:?}", e);
        return Err(CallbackError.into());
    }

    Ok(())
}

pub async fn gzip_encode(message: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(message)?;
    let compressed_bytes = encoder.finish()?;
    Ok(compressed_bytes)
}

pub async fn gzip_decode(compressed_message: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(compressed_message);
    let mut decompressed_bytes = Vec::new();
    decoder.read_to_end(&mut decompressed_bytes)?;
    Ok(decompressed_bytes)
}

pub async fn amqp_main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    let addr =
        std::env::var("SERVICE_BUS_ADDRESS").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());

    async_global_executor::block_on(async {
        let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

        info!("CONNECTED");

        let channel_a = conn.create_channel().await?;
        let channel_b = conn.create_channel().await?;

        let queue = channel_a
            .queue_declare(
                "hello",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!(?queue, "Declared queue");

        let mut consumer = channel_b
            .basic_consume(
                "hello",
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        async_global_executor::spawn(async move {
            info!("will consume");
            while let Some(delivery) = consumer.next().await {
                let delivery = delivery.expect("error in consumer");
                delivery.ack(BasicAckOptions::default()).await.expect("ack");
            }
        })
        .detach();

        let payload = b"Hello world!";

        loop {
            let confirm = channel_a
                .basic_publish(
                    "",
                    "hello",
                    BasicPublishOptions::default(),
                    payload,
                    BasicProperties::default(),
                )
                .await?
                .await?;
            assert_eq!(confirm, Confirmation::NotRequested);
        }
    })
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}