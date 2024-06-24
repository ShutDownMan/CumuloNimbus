use anyhow::{bail, Result};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use futures_lite::stream::StreamExt;
use lapin::{
    options::*, publisher_confirm::Confirmation, types::FieldTable, BasicProperties, Channel,
    Connection, ConnectionProperties, Consumer,
};
use std::{future::IntoFuture, io::prelude::*, sync::{Arc, Mutex}};
use tracing::{debug, info};
use std::collections::HashMap;


pub mod schemas;

// pub use capnp;
// pub use lapin;
pub use capnp::serialize;

#[derive(Debug)]
pub enum MessagePriority {
    Alpha = 2,
    Beta = 1,
    Omega = 0,
}

// TODO: Find a way to create this enum dynamically
#[derive(Debug, Eq, Hash, PartialEq)]
pub enum MessageType {
    PersistDataSeries,
    FetchDataSeries
}

pub type CapnpBuilder<A> = capnp::message::Builder<A>;
pub type CapnpSegmentReader<'a> = capnp::message::Reader<capnp::serialize::SliceSegments<'a>>;
pub type LapinAMQPProperties = lapin::protocol::basic::AMQPProperties;

type MessageHandlerCallbackType = fn(
    CapnpSegmentReader,
    LapinAMQPProperties
) -> Result<()>;

#[derive(Debug)]
struct MessageTypeCallback {
    message_type: MessageType,
    callback: MessageHandlerCallbackType
}

#[derive(Debug)]
pub struct ServiceBus {
    _connection: Connection,
    channel: Channel,
    subscriptions: Arc<Mutex<HashMap<
        String,
        HashMap<MessageType, MessageHandlerCallbackType>
    >>>
}

impl ServiceBus {
    pub async fn new() -> Result<Self> {
        let addr = std::env::var("SERVICE_BUS_ADDRESS")
            .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());

        let connection = Connection::connect(&addr, ConnectionProperties::default()).await?;

        info!("CONNECTED");

        let channel = connection.create_channel().await?;

        let subscriptions = Arc::new(Mutex::new(HashMap::new()));

        Ok(ServiceBus {
            _connection: connection,
            channel,
            subscriptions,
        })
    }

    pub async fn simple_queue_declare(&self, queue_name: &str, routing_key: &str) -> Result<()> {
        info!("declaring queue {}", queue_name);

        let mut fields = FieldTable::default();
        fields.insert("x-max-priority".into(), 2.into());

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
        message_type: MessageType,
        message: &[u8],
        compress: bool,
        priority: Option<MessagePriority>,
    ) -> Result<()> {
        info!("publishing message to queue {}", routing_key);

        debug!("raw message length: {}", message.len());
        let mut properties = BasicProperties::default();
        let mut headers = FieldTable::default();

        headers.insert("message-type".into(), (message_type as u32).into());

        properties = properties.with_headers(headers)
            .with_content_type("application/capnp".into());

        let message = if compress {
            properties = properties.with_content_encoding("gzip".into());
            self.gzip(message).await?
        } else {
            message.to_vec()
        };

        if compress {
            debug!("compressed message length: {}", message.len());
        } else {
            debug!("uncompressed message length: {}", message.len());
        }

        properties = properties.with_priority(priority.unwrap_or(MessagePriority::Omega) as u8);

        let confirm = self
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

    pub async fn gzip(&self, message: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(message)?;
        let compressed_bytes = encoder.finish()?;
        Ok(compressed_bytes)
    }

    pub fn subscribe(&self, topic: &str, message_type: MessageType, callback: MessageHandlerCallbackType) -> Result<()>
    {
        let m_topic: String = topic.to_string();
        let m_subscriptions = self.subscriptions.clone();
        let result = std::thread::spawn(move || {
            let mut subs = m_subscriptions.lock().unwrap();

            // check if exists in hashmap
            if let Some(existing_subs) = subs.get_mut(&m_topic) {
                // if exists, insert into the existing hashmap
                existing_subs.insert(message_type, callback);
            } else {
                // if does not exist, create new entry
                subs.insert(m_topic.clone(), HashMap::from([(message_type, callback)]));
            }
        }).join();

        match result {
            Ok(_) => Ok(()),
            Err(_) => bail!("Could not subscribe to topic {:?}", topic)
        }
    }

    pub async fn listen_forever(&self) -> Result<()> {
        let channel = self._connection.create_channel().await?;
        let m_subscriptions = self.subscriptions.clone();

        let result = std::thread::spawn(move || {
            let subs = m_subscriptions.lock().unwrap();

            let mut consumers = Vec::new();
            for topic in subs.keys() {
                consumers.push(async {channel.basic_consume(
                    topic,
                    "my_consumer",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                ).await});
            }

            let mut consumers = consumers.into_iter()
                .map(|c| async_global_executor::block_on(c.into_future()))
                .map(|c| c.unwrap())
                .collect::<Vec<Consumer>>();

            async_global_executor::block_on(async move {
                let tasks = consumers.into_iter()
                    .map(|mut c| async_global_executor::spawn(async move {
                        while let Some(delivery) = c.next().await {
                            let delivery = delivery.expect("error in consumer");
                            delivery.ack(BasicAckOptions::default()).await.expect("ack");
                        }
                    }))
                    .collect::<Vec<async_global_executor::Task<()>>>();

                    futures::future::join_all(tasks).await;
                }
            );

        }).join();

        match result {
            Ok(_) => Ok(()),
            Err(_) => bail!("Error when listening to messages")
        }
    }

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
    use super::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
