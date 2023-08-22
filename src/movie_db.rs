use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::rc::Rc;

pub mod prelude {
    pub use super::*;
}

/// The `movie_db` error type
#[derive(Debug)]
pub enum DbError {
    AlreadyExists(String),
}

impl Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::AlreadyExists(s) => write!(f, "Movie {s} already exists."),
        }
    }
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
    children: HashMap<char, Rc<RefCell<MovieTrieNode>>>,
    /// A key that represents the possiblity that this node terminates a given string, if `self.key` is the `Some` variant
    /// then the `u32` it holds represents the key for the given movie.
    key: Option<u32>,
}

impl MovieTrieNode {
    fn new() -> Self {
        MovieTrieNode {
            children: HashMap::new(),
            key: None,
        }
    }
}

/// A struct for a trie that will hold all the movies in a way that can be searched efficiently
#[derive(Debug)]
struct MovieTrie {
    root: Rc<RefCell<MovieTrieNode>>,
}

impl MovieTrie {
    /// Creates a new `MovieTrie`
    pub fn new() -> MovieTrie {
        MovieTrie {
            root: Rc::new(RefCell::new(MovieTrieNode::new())),
        }
    }
    /// Searches for a given title in the Trie. Returns an `Option<u32>`,
    /// the return value will be the Some variant if the movie title exists, otherwise it will be the None variant.
    pub fn contains<T: Into<Vec<char>>>(&self, title: T) -> Option<u32> {
        let title = title.into();
        let mut curr = Rc::clone(&self.root);
        let mut end_of_str = true;
        for c in title {
            let next_node = match curr.borrow().children.get(&c) {
                Some(neo) => Rc::clone(neo),
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
    /// Insert a new movie `title` with a given `key` into the Trie
    /// The method is fallable since it is considered an error to enter the same movie twice into the database
    pub fn insert<T: Into<Vec<char>>>(&mut self, title: T, key: u32) -> Result<(), DbError> {
        let title = title.into();
        let movie_display_title = String::from_iter(title.iter());
        let mut curr = Rc::clone(&self.root);
        for c in title {
            let next_node = if curr.borrow().children.contains_key(&c) {
                Rc::clone(curr.borrow().children.get(&c).unwrap())
            } else {
                let neo = Rc::new(RefCell::new(MovieTrieNode::new()));
                curr.borrow_mut().children.insert(c, Rc::clone(&neo));
                neo
            };
            curr = next_node;
        }
        // Check if the string already exists in the Trie
        if curr.borrow().key.is_some() {
            return Err(DbError::AlreadyExists(movie_display_title));
        }
        curr.borrow_mut().key = Some(key);
        Ok(())
    }
    /// Helper function for delete method, not intended to be part of the public interface.
    fn delete_helper(
        &mut self,
        title: &Vec<char>,
        cur_root: Rc<RefCell<MovieTrieNode>>,
        i: usize,
    ) -> (Option<u32>, bool) {
        if i == title.len() {
            let (ret_key, ret_flag) =
                match (cur_root.borrow().key, cur_root.borrow().children.is_empty()) {
                    (Some(k), true) => (Some(k), true),
                    (Some(k), false) => (Some(k), false),
                    (None, true) => (None, true),
                    (None, false) => (None, false),
                };
            return (ret_key, ret_flag);
        } else if cur_root.borrow().children.contains_key(&title[i]) {
            let next_root = Rc::clone(&cur_root.borrow().children[&title[i]]);
            let (key, flag) = self.delete_helper(title, next_root, i + 1);
            let flag = if flag {
                cur_root.borrow_mut().children.remove(&title[i]);
                cur_root.borrow().children.is_empty()
            } else {
                false
            };
            return (key, flag);
        } else {
            return (None, false);
        }
    }
    /// Delete a movie from the trie. The function returns an `Option<u32>` reprenting its unique key of the movie title. The
    /// return value will be the Some variant holding the `u32` that is the unique key for the given movie if it exists in the Trie,
    /// otherwise None will be returned.
    pub fn delete<T: Into<Vec<char>>>(&mut self, title: T) -> Option<u32> {
        let cur_root = Rc::clone(&self.root);
        let title = title.into();
        let (key, _) = self.delete_helper(&title, cur_root, 0);
        key
    }
}

pub struct MovieCollection {
    trie: MovieTrie,
    movie_map: HashMap<u32, (Movie, u64)>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_movie_trie_new() {
        let new_trie = MovieTrie::new();
        println!("{:?}", new_trie);

        assert!(true);
    }

    #[test]
    fn test_movie_trie_insert() {
        let mut new_trie = MovieTrie::new();
        let movie = String::from("Lord of the Rings: The Fellowship of the Ring")
            .chars()
            .collect::<Vec<char>>();
        new_trie.insert(movie, 1);

        println!("{:#?}", new_trie);

        assert!(true);
    }

    #[test]
    fn test_movie_trie_contains() {
        let mut new_trie = MovieTrie::new();
        let movie = String::from("Lord of the Rings: The Fellowship of the Ring")
            .chars()
            .collect::<Vec<char>>();
        new_trie.insert(movie.clone(), 1);

        println!("{:?}", new_trie);

        assert!(new_trie.contains(movie).is_some());
    }

    #[test]
    fn test_movie_trie_delete() {
        let mut new_trie = MovieTrie::new();
        let movie1 = String::from("Lord of the Rings: The Fellowship of the Ring")
            .chars()
            .collect::<Vec<char>>();
        new_trie.insert(movie1.clone(), 1);
        let movie2 = String::from("Lord of the Rings: The Two Towers")
            .chars()
            .collect::<Vec<char>>();
        new_trie.insert(movie2.clone(), 2);
        let movie3 = String::from("Lord of the Rings: The Return of the King")
            .chars()
            .collect::<Vec<char>>();
        new_trie.insert(movie3.clone(), 3);

        if let Some(k) = new_trie.delete(
            String::from("Lord of the Rings: The Return of the King")
                .chars()
                .collect::<Vec<char>>(),
        ) {
            println!(
                "Return of the king deleted: {}",
                new_trie.contains(movie3).is_none()
            );
            assert_eq!(k, 3);

            println!(
                "Trie contains other movies: {}",
                new_trie.contains(movie1.clone()).is_some()
                    && new_trie.contains(movie2.clone()).is_some()
            );

            assert!(new_trie.contains(movie1).is_some() && new_trie.contains(movie2).is_some());
        }
    }
}
