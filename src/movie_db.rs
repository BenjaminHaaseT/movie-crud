use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::default::Default;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::rc::Rc;

pub mod prelude {
    pub use super::*;
}

/// A struct that represents a single movie.
#[derive(Debug, Serialize, Deserialize)]
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
    /// Getter method for the movie's title
    pub fn get_title(&self) -> &String {
        &self.title
    }
}

/// A struct for implementing a Trie data structure
#[derive(Debug)]
struct MovieTrieNode {
    /// Contains all the characters in the current node mapping them to their respective `TrieNode`s if applicable.
    children: HashMap<char, Option<Rc<RefCell<MovieTrieNode>>>>,
    /// A key that represents the possiblity that this node terminates a given string, if `self.key` is the `Some` variant
    /// then the `u32` it holds represents the key for the given movie.
    key: Option<u32>,
}

/// A struct for a trie that will hold all the movies in a way that can be searched efficiently
struct MovieTrie {
    root: Option<Rc<RefCell<MovieTrieNode>>>,
}

impl MovieTrie {
    /// Creates a new `MovieTrie`
    pub fn new(root: Option<Rc<RefCell<MovieTrieNode>>>) -> MovieTrie {
        MovieTrie { root }
    }
    /// Searches for a given title in the Trie. Returns an `Option<u32>`,
    /// the return value will be the Some variant if the movie title exists, otherwise it will be the None variant.
    pub fn search<T: Into<Vec<char>>>(&self, title: T) -> Option<u32> {
        let title = title.into();
        let mut curr = Rc::clone(self.root.as_ref().unwrap());
        let mut end_of_str = true;
        for c in title {
            let next_node = match curr.borrow().children.get(&c) {
                Some(neo) => Rc::clone(neo.as_ref().unwrap()),
                _ => {
                    end_of_str = false;
                    break;
                }
            };
            curr = next_node;
        }
        if end_of_str {
            return curr.borrow().key;
        }
        None
    }
}

impl Default for MovieTrie {
    fn default() -> Self {
        MovieTrie { root: None }
    }
}

pub struct MovieCollection {
    trie: MovieTrie,
    movie_map: HashMap<u32, (Movie, u64)>,
}
