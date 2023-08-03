use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Read, Write};

/// A struct that represents a single movie.
pub struct Movie {
    /// Title of the movie
    title: String,
    /// The rating of the movie out of five stars given by the user
    rating: u8,
    /// A brief description of the movie
    description: String,
    /// A url link to the movie image
    image: String,
}

impl Movie {
    /// Associated method to create a new movie struct
    pub fn new(title: String, rating: u8, description: String, image: String) -> Self {
        Movie {
            title,
            rating,
            description,
            image,
        }
    }
}
