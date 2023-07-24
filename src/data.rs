use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use rocket::fairing::AdHoc;
use rocket::State;
use rocket::serde::{Serialize, Deserialize, json::Json};

use rocket_sync_db_pools::database;

use rusqlite::params;

use rocket_sync_db_pools::rusqlite;
use serde_json::{json, Value};

#[database("rusqlite")]
struct Db(rusqlite::Connection);

struct Cache {
    commodity: HashMap<String, CommodityCache>,
    system: HashMap<u64,SystemCache>
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

    fn get_system(&self, address: u64) -> Option<System> {
        let result = self.system.get(&address);
        return match result {
            None => {
                //Not cached yet
                None
            }
            Some(system) => {
                //10 minutes
                if system.instant.elapsed().as_secs() > 600 {
                    //Cache too old -> sending nothing
                    None
                } else {
                    //Cache usable -> sending
                    Some(system.data.clone())
                }
            }
        };
    }

    fn put_system(&mut self, system: System, address: u64) {
        let system_cache = SystemCache {
            instant: Instant::now(),
            data: system,
        };
        self.system.insert(address, system_cache);
    }
}

#[get("/")]
async fn root() -> &'static str {
    "data"
}

/**
 * System
 **/
struct SystemCache {
    instant: Instant,
    data: System,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
struct System {
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    address: Option<u64>,
    body_count: Option<u64>,
    non_body_count: Option<u64>,
    population: Option<u64>,
    allegiance: Option<String>,
    economy: Option<String>,
    second_economy: Option<String>,
    government: Option<String>,
    security: Option<String>,
    faction: Option<String>,
    x: Option<f64>,
    y: Option<f64>,
    z: Option<f64>,
    planets: Option<Vec<Planet>>,
    stars: Option<Vec<Star>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Planet {
    pub body_name: Option<String>,
    pub body_id: Option<i64>,
    pub distance_from_arrival_ls: Option<f64>,
    pub tidal_lock: Option<bool>,
    pub terraform_state: Option<String>,
    pub planet_class: Option<String>,
    pub atmosphere: Option<String>,
    pub volcanism: Option<String>,
    pub mass_em: Option<f64>,
    pub radius: Option<f64>,
    pub surface_gravity: Option<f64>,
    pub surface_temperature: Option<f64>,
    pub surface_pressure: Option<f64>,
    pub landable: Option<bool>,
    pub semi_major_axis: Option<f64>,
    pub eccentricity: Option<f64>,
    pub orbital_inclination: Option<f64>,
    pub periapsis: Option<f64>,
    pub orbital_period: Option<f64>,
    pub ascending_node: Option<f64>,
    pub mean_anomaly: Option<f64>,
    pub rotation_period: Option<f64>,
    pub axial_tilt: Option<f64>,
    pub was_discovered: Option<bool>,
    pub was_mapped: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Star {
    pub body_name: Option<String>,
    pub body_id: Option<i64>,
    pub distance_from_arrival_ls: Option<f64>,
    pub star_type: Option<String>,
    pub subclass: Option<i64>,
    pub stellar_mass: Option<f64>,
    pub radius: Option<f64>,
    pub absolute_magnitude: Option<f64>,
    pub age_my: Option<i64>,
    pub surface_temperature: Option<f64>,
    pub luminosity: Option<String>,
    pub semi_major_axis: Option<f64>,
    pub eccentricity: Option<f64>,
    pub orbital_inclination: Option<f64>,
    pub periapsis: Option<f64>,
    pub orbital_period: Option<f64>,
    pub ascending_node: Option<f64>,
    pub mean_anomaly: Option<f64>,
    pub rotation_period: Option<f64>,
    pub axial_tilt: Option<f64>,
    pub was_discovered: Option<bool>,
    pub was_mapped: Option<bool>,
}

#[get("/system/<address>")]
async fn system(cache: &State<Arc<Mutex<Cache>>>, db: Db, address: String) -> Option<Json<System>> {
    let address_clone = u64::from_str(address.as_str()).unwrap();
    let some_system = cache.lock().unwrap().get_system(address_clone.clone());
    return match some_system {
        None => {
            let system: System = db.run(move |conn| {

                let mut system = conn.query_row("select name,address,body_count,non_body_count,population,allegiance,economy,second_economy,government,security,faction,x,y,z from system where address = ?",
                                                        params![address_clone.clone()],
                                                        |r| {
                    Ok(System {
                        name: r.get(0)?,
                        address: Option::from(address_clone.clone()),
                        body_count: r.get(2)?,
                        non_body_count: r.get(3)?,
                        population: r.get(4)?,
                        allegiance: r.get(5)?,
                        economy: r.get(6)?,
                        second_economy: r.get(7)?,
                        government: r.get(8)?,
                        security: r.get(9)?,
                        faction: r.get(10)?,
                        x: r.get(11)?,
                        y: r.get(12)?,
                        z: r.get(13)?,
                        planets: None,
                        stars: None,
                    })});

                if system.is_ok(){
                    let mut local_system = system.unwrap();
                    let mut stmnt = conn.prepare("select name,id,distance_from_arrival_ls,type,subclass,stellar_mass,radius,absolute_magnitude,age_my,surface_temperature,luminosity,semi_major_axis,eccentricity,orbital_inclination,periapsis,orbital_period,ascending_node,mean_anomaly,rotation_period,axial_tilt,discovered,mapped from star where system_name = ?").unwrap();
                    let star_iter = stmnt.query_map(params![local_system.name.clone()],|r|{
                        let discovered: String = r.get(20).unwrap();
                        let mapped: String = r.get(21).unwrap();
                        Ok(Star{
                            body_name: r.get(0).ok(),
                            body_id: r.get(1).ok(),
                            distance_from_arrival_ls: r.get(2).ok(),
                            star_type: r.get(3).ok(),
                            subclass: r.get(4).ok(),
                            stellar_mass: r.get(5).ok(),
                            radius: r.get(6).ok(),
                            absolute_magnitude: r.get(7).ok(),
                            age_my: r.get(8).ok(),
                            surface_temperature: r.get(9).ok(),
                            luminosity: r.get(10).ok(),
                            semi_major_axis: r.get(11).ok(),
                            eccentricity: r.get(12).ok(),
                            orbital_inclination: r.get(13).ok(),
                            periapsis: r.get(14).ok(),
                            orbital_period: r.get(15).ok(),
                            ascending_node: r.get(16).ok(),
                            mean_anomaly: r.get(17).ok(),
                            rotation_period: r.get(18).ok(),
                            axial_tilt: r.get(19).ok(),
                            was_discovered: discovered.parse::<bool>().ok(),
                            was_mapped: mapped.parse::<bool>().ok(),
                        })
                    }).unwrap();


                    let mut star_vec: Vec<Star> = vec![];

                    for star in star_iter{
                        star_vec.push(star.unwrap());
                    }


                    local_system.stars = Some(star_vec);

                    let mut stmnt = conn.prepare("select name,id,distance_from_arrival_ls,tidal_lock,terraform_state,class,atmosphere,volcanism,mass_em,radius,surface_gravity,surface_temperature,surface_pressure,landable,semi_major_axis,eccentricity,orbital_inclination,periapsis,orbital_period,ascending_node,mean_anomaly,rotation_period,axial_tilt,discovered,mapped from body where system_name = ?").unwrap();
                    let planet_iter = stmnt.query_map(params![local_system.name.clone()],|r|{
                        let tidal_lock: String = r.get(3).unwrap();
                        let discovered: String = r.get(23).unwrap();
                        let mapped: String = r.get(24).unwrap();
                        Ok(Planet{
                             body_name: r.get(0).ok(),
                             body_id: r.get(1).ok(),
                             distance_from_arrival_ls: r.get(2).ok(),
                             tidal_lock: tidal_lock.parse::<bool>().ok(),
                             terraform_state: r.get(4).ok(),
                             planet_class: r.get(5).ok(),
                             atmosphere: r.get(6).ok(),
                             volcanism: r.get(7).ok(),
                             mass_em: r.get(8).ok(),
                             radius: r.get(9).ok(),
                             surface_gravity: r.get(10).ok(),
                             surface_temperature: r.get(11).ok(),
                             surface_pressure: r.get(12).ok(),
                             landable: r.get(13).ok(),
                             semi_major_axis: r.get(14).ok(),
                             eccentricity: r.get(15).ok(),
                             orbital_inclination: r.get(16).ok(),
                             periapsis: r.get(17).ok(),
                             orbital_period: r.get(18).ok(),
                             ascending_node: r.get(19).ok(),
                             mean_anomaly: r.get(20).ok(),
                             rotation_period: r.get(21).ok(),
                             axial_tilt: r.get(22).ok(),
                            was_discovered: discovered.parse::<bool>().ok(),
                            was_mapped: mapped.parse::<bool>().ok(),
                        })
                    }).unwrap();


                    let mut planet_vec: Vec<Planet> = vec![];

                    for planet in planet_iter{
                        planet_vec.push(planet.unwrap());
                    }


                    local_system.planets = Some(planet_vec);

                    system = Ok(local_system);
                }
                system

            }).await.ok()?;


            //Check if value is there. If not, do not cache! May lead to let memory bloat if there are too many wrong api calls
            match system.name {
                None => {}
                Some(_) => { cache.lock().unwrap().put_system(system.clone(), u64::from_str(address.as_str()).unwrap()); }
            }

            return Some(Json(system));
        }
        Some(system) => {Some(Json(system))}
    };
}

/**
 * Commodity
 **/
struct CommodityCache {
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
       lowest_buy_price,
       lowest_buy_station,
       lowest_buy_system,
       highest_sell_price,
       highest_sell_station,
       highest_sell_system
from commodity

         inner join

     (select max(sell_price) as highest_sell_price, sh.station_name as highest_sell_station, sh.system_name as highest_sell_system
      from commodity hc
               inner join station sh on hc.market_id = sh.market_id
      where hc.name = ?
        and sh.station_name not like '___-___'
      limit 1)

         inner join

     (select min(case when buy_price > 0 then buy_price end) as lowest_buy_price,station_name as lowest_buy_station, system_name as lowest_buy_system
      from station
               inner join commodity lowest_buy_commodity on station.market_id = lowest_buy_commodity.market_id
      where station_name not like '___-___'
        and lowest_buy_commodity.name = ?
      )

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
        system: HashMap::new(),
    };

    let cache_mutex = Arc::new(Mutex::new(cache));

    AdHoc::on_ignite("Data Stage", |rocket| async {
        rocket.attach(Db::fairing()).manage(cache_mutex).mount("/data", routes![root,commodity,system])
    })
}