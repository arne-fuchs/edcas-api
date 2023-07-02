use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use rocket::fairing::AdHoc;
use rocket::State;
use rocket::serde::{Serialize, Deserialize, json::Json};
use rocket::yansi::Color::Default;

use rocket_sync_db_pools::database;

use rusqlite::params;

use rocket_sync_db_pools::rusqlite;

#[database("rusqlite")]
struct Db(rusqlite::Connection);

struct Cache {
    commodity: HashMap<String, CommodityCache>
}

impl Cache {
    fn get_commodity(&self, name: String) -> Option<Commodity>{
        let result = self.commodity.get(name.as_str());
        return match result {
            None => {
                //Not cached yet
                None
            }
            Some(commodity) => {
                //10 minutes
                if commodity.instant.elapsed().as_secs() > 600 {
                    //Cache too old -> sending nothing
                    None
                } else {
                    //Cache usable -> sending
                    Some(commodity.data.clone())
                }
            }
        }
    }

    fn put_commodity(&mut self, commodity: Commodity, name: String){
        let commodity_cache = CommodityCache{
            instant : Instant::now(),
            data: commodity
        };
        self.commodity.insert(name,commodity_cache);
    }
}

#[get("/")]
async fn root() -> &'static str {
    "data"
}


/**
* Commodity
**/
struct CommodityCache {
    instant: Instant,
    data: Commodity
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
struct Commodity {
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    buy_price: Option<f64>,
    sell_price: Option<f64>,
    avg_price: Option<f64>,
}
#[get("/commodity/<name>")]
async fn commodity(cache: &State<Arc<Mutex<Cache>>>, db: Db, name: String) -> Option<Json<Commodity>> {
    let name_clone = name.clone();
    let some_commodity = cache.lock().unwrap().get_commodity(name.clone());
    return match some_commodity {
        None => {
            let commodity = db.run(move |conn| {
                conn.query_row("SELECT avg(buy_price),avg(sell_price),avg(mean_price) from commodity where name = ?", params![name_clone.clone()],
                               |r| Ok(Commodity {
                                   name: Option::from(name_clone),
                                   buy_price: r.get(0)?,
                                   sell_price: r.get(1)?,
                                   avg_price: r.get(2)?
                               }))
            }).await.ok()?;

            cache.lock().unwrap().put_commodity(commodity.clone(),name.clone());

            Some(Json(commodity))
        }
        Some(commodity) => {
            Some(Json(commodity))
        }
    }
}

pub fn stage() -> AdHoc {
    let cache = Cache{
        commodity: HashMap::new(),
    };

    let cache_mutex = Arc::new(Mutex::new(cache));

    AdHoc::on_ignite("Data Stage", |rocket| async {
        rocket.attach(Db::fairing())
            .manage(cache_mutex)
            .mount("/data", routes![root,commodity])
    })
}