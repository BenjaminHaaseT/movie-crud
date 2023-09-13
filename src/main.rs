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
async fn add_movie_handler(collection: web::Data<Mutex<MovieCollection>>, movie: web::Json<Movie>) -> HttpResponse {
    // Get lock on collection struct
    // let mut guard = match collection.lock() {
    //     Ok(g) => g,
    //     Err(e) => return HttpResponse::from_error(UserError::InternalError),
    // };
    // // Add movie to collection
    // let title = movie.get_title().to_owned();
    // match guard.add_record(movie.into_inner()) {
    //     Ok(()) => HttpResponse::Ok().body(format!("{title} added successfully")),
    //     Err(e) => HttpResponse::from_error(e),
    // }

    HttpResponse::Ok().body("OK")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Address, port
    let address = "127.0.0.1";
    let port = 8080;

    // // Create movie collection struct
    // let mut movie_collection = MovieCollection::new();
    // if let Err(e) = movie_collection.load("db/db.txt") {
    //     eprintln!("{e}");
    //     std::process::exit(1);
    // }
    // let app_state = web::Data::new(Mutex::new(movie_collection));
    //
    // HttpServer::new(move || {
    //     App::new().service(
    //         web::resource("/")
    //             .app_data(app_state.clone())
    //     )
    // })
    //     .bind((address, port))?
    //     .run()
    //     .await

    Ok(())
}
