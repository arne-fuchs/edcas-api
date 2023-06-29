use rocket::fairing::AdHoc;
use rocket::response::Debug;
use rocket::{Build, Rocket};
use rocket::serde::{Serialize, Deserialize, json::Json};

use rocket_sync_db_pools::database;

use rusqlite::params;

use rocket_sync_db_pools::rusqlite;
use serde_json::json;

#[database("rusqlite")]
struct Db(rusqlite::Connection);

type Result<T, E = Debug<rusqlite::Error>> = std::result::Result<T, E>;

#[get("/")]
async fn root(_db: Db) -> &'static str {
    "data"
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
struct Data {
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    buy_price: Option<f64>,
    sell_price: Option<f64>,
    avg_price: Option<f64>,
}

#[get("/commodity/<name>")]
async fn commodity(db: Db, name: String) -> Option<Json<Data>> {
    let name_clone = name.clone();
    let post = db.run(move |conn| {
        conn.query_row("SELECT avg(buy_price),avg(sell_price),avg(mean_price) from commodity where name = ?", params![name],
                       |r| Ok(Data{
                           name: Option::from(name_clone),
                           buy_price: r.get(0)?,
                           sell_price: r.get(1)?,
                           avg_price: r.get(2)?
                       }))
    }).await.ok()?;
    Some(Json(post))
}

pub fn stage() -> AdHoc {
    AdHoc::on_ignite("Data Stage", |rocket| async {
        rocket.attach(Db::fairing())
            .mount("/data", routes![root,commodity])
    })
}