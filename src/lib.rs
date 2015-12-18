#[macro_use]
extern crate hyper;
extern crate serde_json;
extern crate chrono;

use hyper::{Client};
use serde_json::{Value};
use chrono::naive::datetime::{NaiveDateTime};
use std::iter::{FromIterator};


#[derive(Debug)]
pub struct VenueInfo {
    pub id: u64,
    pub name: String,
    pub is_open: bool,
    pub venue: String,
}

#[derive(Copy, Clone, Debug)]
pub struct Order {
    pub price: u64,
    pub qty: u64,
    pub is_buy: bool,
}

#[derive(Debug)]
pub struct Orderbook {
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
    pub timestamp: NaiveDateTime,
}


pub type StockfighterResult<T> = Result<T, String>;
pub trait StockfighterAPI {
    fn heartbeat(&self) -> StockfighterResult<()>;
    fn venues(&self) -> StockfighterResult<Vec<VenueInfo>>;

    fn venue_heartbeat(&self, venue: &str) -> StockfighterResult<()>;
    fn stock_orderbook(&self, venue: &str, stock: &str) -> StockfighterResult<Orderbook>;
}


#[derive(Copy, Clone, Debug)]
pub struct StockfighterHttpApi {
    pub base_url: &'static str,
    pub api_key: &'static str,
}

header! { (XStarfighterAuthorization, "X-Starfighter-Authorization") => [String] }

impl StockfighterHttpApi {
    #[allow(unused_parens)]
    pub fn send_raw(&self, path: &str) -> StockfighterResult<Value> {
        let url = format!("{}{}", self.base_url, path);

        let client = Client::new();
        let req =
            ( client
            . get(&url)
            . header(XStarfighterAuthorization(self.api_key.to_owned()))
            );

        let mut res = match req.send() {
            Ok(res) => res,
            Err(_) => return Err("Error sending request".to_owned()),
        };

        let json = match serde_json::from_reader(&mut res) {
            Ok(json) => Ok(json),
            Err(_) => return Err("Response body invalid".to_owned()),
        };

        // println!("{:#?}", json);
        json
    }
}

impl StockfighterAPI for StockfighterHttpApi {
    fn heartbeat(&self) -> StockfighterResult<()> {
        let response = self.send_raw("/heartbeat").unwrap();
        let json = response.as_object().unwrap();

        let ok = json.get("ok").unwrap().as_boolean().unwrap();
        if !ok {
            return Err(json.get("error").unwrap().as_string().unwrap().to_owned());
        }

        Ok(())
    }

    fn venues(&self) -> StockfighterResult<Vec<VenueInfo>> {
        let response = self.send_raw("/venues").unwrap();
        let json = response.as_object().unwrap();

        // This API call gives an `id` boolean field instead of an `ok` boolean field.
        // I suspect this is a bug...
        let ok = json.get("id").unwrap().as_boolean().unwrap();
        if !ok {
            return Err(json.get("error").unwrap().as_string().unwrap().to_owned());
        }

        let venues = json.get("venues").unwrap().as_array().unwrap().into_iter().map(|venue| {
            let is_open = {
                let state = venue.as_object().unwrap().get("state").unwrap().as_string().unwrap();
                if state == "open" {
                    true
                } else if state == "closed" {
                    false
                } else {
                    panic!(format!("Unexpected value for venue state: '{}'", state))
                }
            };

            VenueInfo {
                id: venue.as_object().unwrap().get("id").unwrap().as_u64().unwrap(),
                name: venue.as_object().unwrap().get("name").unwrap().as_string().unwrap().to_owned(),
                is_open: is_open,
                venue: venue.as_object().unwrap().get("venue").unwrap().as_string().unwrap().to_owned(),
            }
        });

        Ok(Vec::from_iter(venues))
    }

    fn venue_heartbeat(&self, venue: &str) -> StockfighterResult<()> {
        let path = format!("/venues/{}/heartbeat", venue);

        let response = self.send_raw(&*path).unwrap();
        let json = response.as_object().unwrap();

        let ok = json.get("ok").unwrap().as_boolean().unwrap();
        if !ok {
            return Err(json.get("error").unwrap().as_string().unwrap().to_owned());
        }

        Ok(())
    }

    fn stock_orderbook(&self, venue: &str, stock: &str) -> StockfighterResult<Orderbook> {
        let path = format!("/venues/{}/stocks/{}", venue, stock);

        let response = self.send_raw(&*path).unwrap();
        let json = response.as_object().unwrap();

        let ok = json.get("ok").unwrap().as_boolean().unwrap();
        if !ok {
            return Err(json.get("error").unwrap().as_string().unwrap().to_owned());
        }

        let bids = json.get("bids").unwrap().as_array().unwrap().into_iter().map(|bid| {
            Order {
                price:  bid.as_object().unwrap().get("price").unwrap().as_u64().unwrap(),
                qty:    bid.as_object().unwrap().get("qty").unwrap().as_u64().unwrap(),
                is_buy: true,
            }
        });

        let asks = json.get("asks").unwrap().as_array().unwrap().into_iter().map(|ask| {
            Order {
                price:  ask.as_object().unwrap().get("price").unwrap().as_u64().unwrap(),
                qty:    ask.as_object().unwrap().get("qty").unwrap().as_u64().unwrap(),
                is_buy: false,
            }
        });

        let timestamp = NaiveDateTime::parse_from_str(
            json.get("ts").unwrap().as_string().unwrap(),
            "%+"
        ).unwrap();

        Ok(Orderbook {
            bids: Vec::from_iter(bids),
            asks: Vec::from_iter(asks),
            timestamp: timestamp,
        })
    }
}
