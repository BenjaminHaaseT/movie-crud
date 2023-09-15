mod movie_db;
use std::sync::{Mutex, Arc};
use std::fmt::{Display, Formatter};
use std::error::Error;
use actix_web::{http, web, App, HttpRequest, HttpResponse, HttpServer, get, post, Responder, http::header::ContentType};
use actix_web::body::BoxBody;
use movie_db::prelude::*;
use serde_json;
use atomic_wait::{wake_one, wait, wake_all};

impl Responder for Movie {
    type Body = BoxBody;
    fn respond_to(self, req: &HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(&self).unwrap();
        HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(body)
    }
}


#[derive(Debug)]
enum UserError {
    InternalError,
    NotFound(String),

}

impl Display for UserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UserError::InternalError => write!(f, "An internal server error occurred"),
            UserError::NotFound(title) => write!(f, "no record with title: {title}"),
        }
    }
}

impl Error for UserError {}


/// Handler that allows users of api to add a movie to the collection
#[post("/add-movie")]
async fn add_movie_handler(collection: web::Data<DbLock>, movie: web::Json<Movie>) -> HttpResponse {
    // Lock the collection struct
    let mut guard = collection.lock();
    if let Err(e) = guard.add_record(movie.into_inner()) {
        return HttpResponse::from_error(UserError::InternalError);
    }
    HttpResponse::Ok().body("OK")
}

/// Handler that allows one to query the collection for a specific movie title
#[get("/find-movie/{title}")]
async fn find_movie(collection: web::Data<DbLock>, query_title: web::Query<String>) -> impl Responder {
    // Get title string from query
    let title = query_title.into_inner();
    // lock the mutex
    let guard = collection.lock();
    match guard.find(title.clone()) {
        Some(id) => Ok(guard.get_movie(id).unwrap().clone()),
        None => Err(UserError::NotFound(title))
    }
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
