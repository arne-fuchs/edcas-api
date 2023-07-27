use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use rocket::fairing::AdHoc;
use rocket::State;
use rocket::serde::{Serialize, Deserialize, json::Json};

use rocket_sync_db_pools::database;


use rocket_sync_db_pools::postgres;
use serde_json::{json, Value};

#[database("postgres_db")]
struct DbConn(postgres::Client);

struct Cache {
    commodity: HashMap<String, CommodityCache>,
    system: HashMap<i64, SystemCache>,
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

    fn get_system(&self, address: i64) -> Option<System> {
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

    fn put_system(&mut self, system: System, address: i64) {
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
    address: Option<i64>,
    body_count: Option<i32>,
    non_body_count: Option<i32>,
    population: Option<i32>,
    allegiance: Option<String>,
    economy: Option<String>,
    second_economy: Option<String>,
    government: Option<String>,
    security: Option<String>,
    faction: Option<String>,
    x: Option<f32>,
    y: Option<f32>,
    z: Option<f32>,
    planets: Option<Vec<Planet>>,
    stars: Option<Vec<Star>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Planet {
    pub body_name: Option<String>,
    pub body_id: Option<i32>,
    pub distance_from_arrival_ls: Option<f32>,
    pub tidal_lock: Option<bool>,
    pub terraform_state: Option<String>,
    pub planet_class: Option<String>,
    pub atmosphere: Option<String>,
    pub volcanism: Option<String>,
    pub mass_em: Option<f32>,
    pub radius: Option<f32>,
    pub surface_gravity: Option<f32>,
    pub surface_temperature: Option<f32>,
    pub surface_pressure: Option<f32>,
    pub landable: Option<bool>,
    pub semi_major_axis: Option<f32>,
    pub eccentricity: Option<f32>,
    pub orbital_inclination: Option<f32>,
    pub periapsis: Option<f32>,
    pub orbital_period: Option<f32>,
    pub ascending_node: Option<f32>,
    pub mean_anomaly: Option<f32>,
    pub rotation_period: Option<f32>,
    pub axial_tilt: Option<f32>,
    pub was_discovered: Option<bool>,
    pub was_mapped: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Star {
    pub body_name: Option<String>,
    pub body_id: Option<i32>,
    pub distance_from_arrival_ls: Option<f32>,
    pub star_type: Option<String>,
    pub subclass: Option<i32>,
    pub stellar_mass: Option<f32>,
    pub radius: Option<f32>,
    pub absolute_magnitude: Option<f32>,
    pub age_my: Option<i32>,
    pub surface_temperature: Option<f32>,
    pub luminosity: Option<String>,
    pub semi_major_axis: Option<f32>,
    pub eccentricity: Option<f32>,
    pub orbital_inclination: Option<f32>,
    pub periapsis: Option<f32>,
    pub orbital_period: Option<f32>,
    pub ascending_node: Option<f32>,
    pub mean_anomaly: Option<f32>,
    pub rotation_period: Option<f32>,
    pub axial_tilt: Option<f32>,
    pub was_discovered: Option<bool>,
    pub was_mapped: Option<bool>,
}

#[get("/<dlc>/system/<address>")]
async fn system(cache: &State<Arc<Mutex<Cache>>>, db: DbConn, address: i64, dlc: String) -> Option<Json<System>> {
    let odyssey = dlc.contains("odyssey");
    let some_system = cache.lock().unwrap().get_system(address);
    return match some_system {
        None => {
            let system: Option<System> = db.run(move |conn| {

                //language=postgresql
                let row_option = conn.query_one("select name,address,body_count,non_body_count,population,allegiance,economy,second_economy,government,security,faction,x,y,z from system where address = $1 and odyssey = $2",
                                                &[&address, &odyssey]).ok();

                if let Some(row) = row_option {
                    let mut local_system = System {
                        name: row.get(0),
                        address: Option::from(address),
                        body_count: row.get(2),
                        non_body_count: row.get(3),
                        population: row.get(4),
                        allegiance: row.get(5),
                        economy: row.get(6),
                        second_economy: row.get(7),
                        government: row.get(8),
                        security: row.get(9),
                        faction: row.get(10),
                        x: row.get(11),
                        y: row.get(12),
                        z: row.get(13),
                        planets: None,
                        stars: None,
                    };

                    //language=postgresql
                    let mut sql = "select name,id,distance_from_arrival_ls,type,subclass,stellar_mass,
                        radius,absolute_magnitude,age_my,surface_temperature,luminosity,semi_major_axis,eccentricity,
                        orbital_inclination,periapsis,orbital_period,ascending_node,mean_anomaly,rotation_period,
                        axial_tilt,discovered,mapped from star where system_address = $1 and odyssey = $2";

                    let stars_option = conn.query(sql, &[&address, &odyssey]).ok();

                    if let Some(stars) = stars_option {
                        let mut star_vec: Vec<Star> = vec![];

                        for r in stars {
                            let discovered = r.get(20);
                            let mapped = r.get(21);
                            star_vec.push(Star {
                                body_name: r.get(0),
                                body_id: Some(r.get(1)),
                                distance_from_arrival_ls: Some(r.get(2)),
                                star_type: r.get(3),
                                subclass: r.get(4),
                                stellar_mass: r.get(5),
                                radius: r.get(6),
                                absolute_magnitude: r.get(7),
                                age_my: r.get(8),
                                surface_temperature: r.get(9),
                                luminosity: r.get(10),
                                semi_major_axis: r.get(11),
                                eccentricity: r.get(12),
                                orbital_inclination: r.get(13),
                                periapsis: r.get(14),
                                orbital_period: r.get(15),
                                ascending_node: r.get(16),
                                mean_anomaly: r.get(17),
                                rotation_period: r.get(18),
                                axial_tilt: r.get(19),
                                was_discovered: discovered,
                                was_mapped: mapped,
                            });
                        }

                        local_system.stars = Some(star_vec);
                    }

                    //language=postgresql
                    sql = "select name,id,distance_from_arrival_ls,tidal_lock,terraform_state,class,atmosphere,volcanism,mass_em,radius,surface_gravity,surface_temperature,surface_pressure,
                    landable,semi_major_axis,eccentricity,orbital_inclination,periapsis,orbital_period,ascending_node,mean_anomaly,rotation_period,axial_tilt,discovered,mapped from body where system_address = $1 and odyssey = $2";

                    let planets = conn.query(sql, &[&address, &odyssey]).unwrap();


                    //if let Some(planets) = planets_options {
                        let mut planet_vec: Vec<Planet> = vec![];

                        for r in planets {
                            let tidal_lock = r.get(3);
                            let discovered: Option<bool> = r.get(23);
                            let mapped: Option<bool> = r.get(24);

                            planet_vec.push(Planet {
                                body_name: r.get(0),
                                body_id: r.get(1),
                                distance_from_arrival_ls: r.get(2),
                                tidal_lock,
                                terraform_state: r.get(4),
                                planet_class: r.get(5),
                                atmosphere: r.get(6),
                                volcanism: r.get(7),
                                mass_em: r.get(8),
                                radius: r.get(9),
                                surface_gravity: r.get(10),
                                surface_temperature: r.get(11),
                                surface_pressure: r.get(12),
                                landable: r.get(13),
                                semi_major_axis: r.get(14),
                                eccentricity: r.get(15),
                                orbital_inclination: r.get(16),
                                periapsis: r.get(17),
                                orbital_period: r.get(18),
                                ascending_node: r.get(19),
                                mean_anomaly: r.get(20),
                                rotation_period: r.get(21),
                                axial_tilt: r.get(22),
                                was_discovered: discovered,
                                was_mapped: mapped,
                            });
                        }
                        local_system.planets = Some(planet_vec);
                    //}
                    return Some(local_system);
                }
                None
            }).await;

            //Check if value is there. If not, do not cache! May lead to let memory bloat if there are too many wrong api calls
            if let Some(system) = system {
                let local_system = system.clone();
                cache.lock().unwrap().put_system(local_system.clone(), local_system.address.unwrap());
                return Some(Json(local_system));
            }
            return None;
        }
        Some(system) => { Some(Json(system)) }
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
    buy_price: i32,
    sell_price: i32,
    mean_price: i32,
    lowest_buy_price: Value,
    highest_sell_price: Value,
}

//TODO Handle stuff like drones where there are no stations etc.
#[get("/<dlc>/commodity/<name>")]
async fn commodity(cache: &State<Arc<Mutex<Cache>>>, db: DbConn, name: String, dlc: String) -> Option<Json<Commodity>> {
    let name_clone = name.clone();
    let dlc_clone = dlc.contains("odyssey");
    let some_commodity = cache.lock().unwrap().get_commodity(name.clone());
    return match some_commodity {
        None => {
            let data = db.run(move |conn| {
                //language=postgresql
                let sql = "
                    SELECT DISTINCT
                                CAST(AVG(buy_price) OVER () as INTEGER) as avg_buy_price,
                                CAST(AVG(sell_price) OVER () as INTEGER) as avg_sell_price,
                                CAST(AVG(mean_price) OVER () as INTEGER) as avg_mean_price,
                                lowest_buy_price,
                                lowest_buy_station,
                                lowest_buy_system,
                                highest_sell_price,
                                highest_sell_station,
                                highest_sell_system
                    FROM commodity
                             INNER JOIN (
                        SELECT sell_price as highest_sell_price,
                               sh.name as highest_sell_station,
                               sh.system_name as highest_sell_system,
                               ROW_NUMBER() OVER (ORDER BY sell_price DESC) as rn
                        FROM commodity hc
                                 INNER JOIN station sh ON hc.market_id = sh.market_id
                        WHERE hc.name = $1 AND hc.odyssey = $2
                          AND sh.name NOT LIKE '___-___'
                    ) AS highest_sell
                                        ON 1=1 -- Dummy join to get a Cartesian product (all combinations)
                             INNER JOIN (
                        SELECT CASE WHEN buy_price > 0 THEN buy_price END as lowest_buy_price,
                               lb.name as lowest_buy_station,
                               lb.system_name as lowest_buy_system,
                               ROW_NUMBER() OVER (ORDER BY buy_price) as rn
                        FROM station lb
                                 INNER JOIN commodity lowest_buy_commodity ON lb.market_id = lowest_buy_commodity.market_id
                        WHERE lb.name NOT LIKE '___-___'
                          AND lowest_buy_commodity.name = $1 AND lowest_buy_commodity.odyssey = $2 AND lowest_buy_commodity.buy_price > 0
                    ) AS lowest_buy
                                        ON 1=1 -- Dummy join to get a Cartesian product (all combinations)
                    WHERE name = $1 AND odyssey = $2
                      AND lowest_buy.rn = 1
                      AND highest_sell.rn = 1;
                    ";
                let r = conn.query_one(sql, &[&name_clone, &dlc_clone]).unwrap();

                //if let Some(r) = optional_row {
                    let lowest_buy_data = json!(
                        {
                            "buy_price": r.get::<usize,i32>(3),
                            "station": r.get::<usize,String>(4),
                            "system": r.get::<usize,String>(5),
                        }
                    );
                    let highest_sell_data = json!(
                        {
                            "sell_price": r.get::<usize,i32>(6),
                            "station": r.get::<usize,String>(7),
                            "system": r.get::<usize,String>(8),
                        }
                    );
                    let commodity = Commodity {
                        name: Option::from(name_clone),
                        buy_price: r.get(0),
                        sell_price: r.get(1),
                        mean_price: r.get(2),
                        lowest_buy_price: lowest_buy_data,
                        highest_sell_price: highest_sell_data,
                    };
                    return Some(commodity);
                //}
                None
            }).await;

            if let Some(commodity) = data {
                cache.lock().unwrap().put_commodity(commodity.clone(), name.clone());
                return Some(Json(commodity));
            }

            return None;
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
            rocket.attach(DbConn::fairing()).manage(cache_mutex).mount("/data", routes![root,commodity,system])
        })
    }