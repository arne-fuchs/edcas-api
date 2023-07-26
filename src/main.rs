mod data;
use dotenv::dotenv;

//noinspection RsMainFunctionNotFound
#[macro_use] extern crate rocket;

use rocket::{Build, Rocket};

#[get("/ping")]
fn ping() -> &'static str {
    "pong"
}


#[launch]
fn rocket() -> Rocket<Build> {
    dotenv().expect(".env file not found");
    rocket::build()
        .mount("/", routes![ping])
        .attach(data::stage())
}