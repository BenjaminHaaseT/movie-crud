mod movie_db;

use actix_web::{http, web, App, HttpRequest, HttpResponse, HttpServer};
use movie_db::prelude::*;

/// Handler to add a movie to our database
async fn add_movie_handler(req: HttpRequest, movie_data: web::Json<Movie>) -> HttpResponse {
    // TODO: Ensure movie has not already been added to the database

    let movie = movie_data.into_inner();
    let title = movie.get_title();
    // TODO: insert movie into database struct

    HttpResponse::Ok().body("succesessfully added {title}.")
}

fn main() {
    println!("Hello, world!");
}
