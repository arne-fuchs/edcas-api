use std::cell::RefCell;
use std::collections::HashMap;
use std::env::var;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use json::JsonValue;
use rocket::fairing::AdHoc;
use rocket::State;
use rocket::serde::{Serialize, Deserialize, json::Json};
use rocket::yansi::Color::Default;

use rocket_sync_db_pools::database;

use rusqlite::params;

use rocket_sync_db_pools::rusqlite;
use serde_json::{json, Value};

#[database("rusqlite")]
struct Db(rusqlite::Connection);

struct Cache {
    commodity: HashMap<String, CommodityCache>,
}

impl Cache {
    fn get_commodity(&self, name: String) -> Option<Commodity> {
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
        };
    }

    fn put_commodity(&mut self, commodity: Commodity, name: String) {
        let commodity_cache = CommodityCache {
            instant: Instant::now(),
            data: commodity,
        };
        self.commodity.insert(name, commodity_cache);
    }
}

#[get("/")]
async fn root() -> &'static str {
    "data"
}


/**
 * Commodity
 **/ struct CommodityCache {
    instant: Instant,
    data: Commodity,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
struct Commodity {
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    buy_price: Option<f64>,
    sell_price: Option<f64>,
    mean_price: Option<f64>,
    lowest_buy_price: Option<Value>,
    highest_sell_price: Option<Value>,
}

//TODO Handle stuff like drones where there are no stations etc.
#[get("/commodity/<name>")]
async fn commodity(cache: &State<Arc<Mutex<Cache>>>, db: Db, name: String) -> Option<Json<Commodity>> {
    let name_clone = name.clone();
    let some_commodity = cache.lock().unwrap().get_commodity(name.clone());
    return match some_commodity {
        None => {
            let commodity: Commodity = db.run(move |conn| {
                conn.query_row("
                select avg(buy_price),
       avg(sell_price),
       avg(mean_price),
       min(case when buy_price > 0 and stock > 1000 then buy_price end) as lowest_buy_price,
       lowest_buy_station,
       lowest_buy_system,
       max(sell_price) as highest_sell_price,
       highest_sell_station,
       highest_sell_system
from commodity
        left join
     (select station_name as highest_sell_station, system_name as highest_sell_system
      from station
      where station.market_id = (select market_id
                                 from commodity
                                 where sell_price = (select max(sell_price)
                                                     from commodity
                                                     where name = ? limit 1)))
                                                     left join
     (select station_name as lowest_buy_station, system_name as lowest_buy_system
      from station
      where station.market_id = (select market_id
                                 from commodity
                                 where buy_price = (select min(case when buy_price > 0 then buy_price end)
                                                     from commodity
                                                     where name = ? and stock > 1000 limit 1)))

where name = ?;
", params![name_clone.clone(),name_clone.clone(),name_clone.clone()],
                               |r| {
                                   let lowest_buy_data = json!(
                                       {
                                           "buy_price": r.get::<usize,u64>(3).unwrap_or(0),
                                           "station": r.get::<usize,String>(4).unwrap_or(String::from("null")),
                                           "system": r.get::<usize,String>(5).unwrap_or(String::from("null"))
                                       }
                                   );
                                   let highest_sell_data = json!(
                                       {
                                           "sell_price": r.get::<usize,u64>(6).unwrap_or(0),
                                           "station": r.get::<usize,String>(7).unwrap_or(String::from("null")),
                                           "system": r.get::<usize,String>(8).unwrap_or(String::from("null"))
                                       }
                                   );
                                   Ok(Commodity {
                                       name: Option::from(name_clone),
                                       buy_price: r.get(0)?,
                                       sell_price: r.get(1)?,
                                       mean_price: r.get(2)?,
                                       lowest_buy_price: Some(lowest_buy_data),
                                       highest_sell_price: Some(highest_sell_data),
                                   })
                               })
            }).await.ok()?;

            //Check if value is there. If not, do not cache! May lead to let memory bloat if there are too many wrong api calls
            match commodity.mean_price {
                None => {}
                Some(_) => { cache.lock().unwrap().put_commodity(commodity.clone(), name.clone()); }
            }

            Some(Json(commodity))
        }
        Some(commodity) => {
            Some(Json(commodity))
        }
    };
}

pub fn stage() -> AdHoc {
    let cache = Cache {
        commodity: HashMap::new(),
    };

    let cache_mutex = Arc::new(Mutex::new(cache));

    AdHoc::on_ignite("Data Stage", |rocket| async {
        rocket.attach(Db::fairing()).manage(cache_mutex).mount("/data", routes![root,commodity])
    })
}