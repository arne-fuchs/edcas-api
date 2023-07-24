mod data;

//noinspection RsMainFunctionNotFound
#[macro_use] extern crate rocket;

use rocket::{Build, Rocket};

#[get("/ping")]
fn ping() -> &'static str {
    "pong"
}


#[launch]
fn rocket() -> Rocket<Build> {
    rocket::build()
        .mount("/", routes![ping])
        .attach(data::stage())
}