use anyhow::Result;
use futures_lite::stream::StreamExt;
use lapin::{
    options::*, publisher_confirm::Confirmation, types::FieldTable, BasicProperties, Channel,
    Connection, ConnectionProperties,
};
use tracing::{debug, info};

pub use capnp;
pub mod schemas;

pub struct ServiceBus {
    connection: Connection,
    channel: Channel,
}

impl ServiceBus {
    pub async fn new() -> Result<Self> {
        let addr = std::env::var("SERVICE_BUS_ADDRESS")
            .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());

        let connection = Connection::connect(&addr, ConnectionProperties::default()).await?;

        info!("CONNECTED");

        let channel = connection.create_channel().await?;

        Ok(ServiceBus {
            connection,
            channel,
        })
    }

    pub async fn queue_declare(&self, queue_name: &str) -> Result<()> {
        info!("declaring queue {}", queue_name);

        let queue = self
            .channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!(?queue, "Declared queue");

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
                BasicProperties::default().with_delivery_mode(2),
            )
            .await?
            .await?;

        assert_eq!(confirm, Confirmation::NotRequested);

        Ok(())
    }

    // publish message with capnp
    pub async fn capnp_publish_message(&self, queue_name: &str, message: &[u8]) -> Result<()> {
        info!("publishing message to queue {}", queue_name);

        debug!("message length: {}", message.len());

        let confirm = self
            .channel
            .basic_publish(
                "",
                queue_name,
                BasicPublishOptions::default(),
                message,
                BasicProperties::default().with_content_type("application/capnp".into()),
            )
            .await?
            .await?;

        assert_eq!(confirm, Confirmation::NotRequested);

        Ok(())
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
