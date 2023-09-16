mod movie_db;
use actix_web::body::BoxBody;
use actix_web::http::StatusCode;
use actix_web::{
    error, get, guard, http, http::header::ContentType, post, web, App, HttpRequest, HttpResponse,
    HttpServer, Responder, ResponseError, put,
};
use atomic_wait::{wait, wake_all, wake_one};
use movie_db::prelude::*;
use serde_json;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

impl Responder for Movie {
    type Body = BoxBody;
    fn respond_to(self, req: &HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(&self).unwrap();
        HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(body)
    }
}

impl Responder for &Movie {
    type Body = BoxBody;
    fn respond_to(self, req: &HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(self).unwrap();
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
impl ResponseError for UserError {
    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::build(self.status_code())
            .content_type(ContentType::html())
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match self {
            UserError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            UserError::NotFound(_) => StatusCode::BAD_REQUEST,
        }
    }
}

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

/// Handler that allows user to delete a movie
#[put("/delete-movie/{title}")]
async fn delete_movie_handler(collection: web::Data<DbLock>, title: web::Query<String>) -> impl Responder {
    // Lock to get a guard
    let mut guard = collection.lock();
    let title = title.into_inner();
    let id = if let Some(id) = guard.find(title.clone()) {
        id
    } else {
        return Err(UserError::NotFound(title))
    };
    guard.delete(id).map_err(|_e| UserError::InternalError)?;
    Ok("Successfully deleted {title}")
}
/// Handler that allows one to query the collection for a specific movie title
#[get("/find-movie/{title}")]
async fn find_movie_handler(
    collection: web::Data<DbLock>,
    query_title: web::Query<String>,
) -> impl Responder {
    // Get title string from query
    let title = query_title.into_inner();
    // lock the mutex
    let guard = collection.lock();
    match guard.find(title.clone()) {
        Some(id) => Ok(guard.get_movie(id).unwrap().clone()),
        None => Err(UserError::NotFound(title)),
    }
}

/// Handler that will return json string of all movies in the collection
#[get("/get-all-movies")]
async fn get_all_movie_handler(collection: web::Data<DbLock>) -> impl Responder {
    // Lock mutex to ensure lifetime of mutex is long enough
    let guard = collection.lock();
    let movies = guard.get_all_movies();
    serde_json::to_string(&movies)
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
            .service(find_movie_handler)
            .service(add_movie_handler)
    })
    .bind((address, port))?
    .run()
    .await
}
