mod movie_db;
use std::sync::{Mutex, Arc};
use std::fmt::{Display, Formatter};
use std::error::Error;
use actix_web::{http, web, App, HttpRequest, HttpResponse, HttpServer, get, post};
use movie_db::prelude::*;
use atomic_wait::{wake_one, wait, wake_all};


#[derive(Debug)]
enum UserError {
    InternalError

}

impl Display for UserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            UserError::InternalError => write!(f, "An internal server error occurred"),
        }
    }
}

impl Error for UserError {}



#[post("/add-movie")]
async fn add_movie_handler(collection: web::Data<DbLock>, movie: web::Json<Movie>) -> HttpResponse {
    // Lock the collection struct
    let mut guard = collection.lock();
    if let Err(e) = guard.add_record(movie.into_inner()) {
        return HttpResponse::from_error(UserError::InternalError);
    }
    HttpResponse::Ok().body("OK")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Address, port
    let address = "127.0.0.1";
    let port = 8080;
    // Create Dblock
    let dblock = if let Ok(lock) = DbLock::load("db/db.txt") {
        lock
    } else {
        eprintln!("error loading file into lock");
        std::process::exit(1);
    };
    // Instantiate application data
    let app_data = web::Data::new(dblock);
    HttpServer::new(move || {
        App::new()
            .app_data(app_data.clone())
            .service(add_movie_handler)
    })
        .bind((address, port))?
        .run()
        .await
}
