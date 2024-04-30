use adnl::{AdnlPeer, AdnlRawPublicKey};
use tokio_tower::multiplex;
use ton_liteapi::layers::WrapMessagesLayer;
use ton_liteapi::peer::LitePeer;
use ton_liteapi::tl::request::{Request, WrappedRequest};
use tower::{Service, ServiceBuilder, ServiceExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_public = AdnlRawPublicKey::try_from(&*hex::decode("691a14528fb2911839649c489cb4cbec1f4aa126c244c0ea2ac294eb568a7037")?)?;
    let ls_ip = "127.0.0.1";
    let ls_port: u16 = 8080;
    let adnl = AdnlPeer::connect(&server_public, (ls_ip, ls_port)).await?;
    let lite = LitePeer::new(adnl);
    let mut service = ServiceBuilder::new()
        .layer(WrapMessagesLayer)
        .service(multiplex::Client::<_, Box<dyn std::error::Error + Send + Sync + 'static>, _>::new(lite));
    let message = WrappedRequest { wait_masterchain_seqno: None, request: Request::GetTime };
    let result = service.ready().await?.call(message.clone()).await?;
    println!("{:?}", result);
    let result = service.ready().await?.call(message.clone()).await?;
    println!("{:?}", result);
    let result = service.ready().await?.call(message.clone()).await?;
    println!("{:?}", result);
    Ok(())
}