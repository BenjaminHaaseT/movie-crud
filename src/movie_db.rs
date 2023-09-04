use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, BufReader, Read, Write, Seek, SeekFrom};
use std::path::Path;
use std::rc::Rc;




pub mod prelude {
    pub use super::*;
}

/// The `movie_db` error type
#[derive(Debug)]
pub enum DbError {
    AlreadyExists(String),
    FileDoesNotExist(String),
    LoadError
}

impl Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::AlreadyExists(s) => write!(f, "Movie {s} already exists."),
            DbError::FileDoesNotExist(s) => write!(f, "File {s} does not exist."),
            DbError::LoadError => write!(f, "Error loading file."),
        }
    }
}

/// A struct that represents a single movie.
#[derive(Debug, Serialize, Deserialize)]
pub struct Movie {
    /// The rating of the movie out of five stars given by the user.
    rating: u8,
    /// Title of the movie.
    title: String,
    /// A brief description of the movie.
    description: String,
    /// A url link to the movie image.
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
    /// A key that represents the possibility that this node terminates a given string, if `self.key` is the `Some` variant
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
            return (cur_root.borrow().key, cur_root.borrow().children.is_empty());
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
    /// Delete a movie from the trie. The function returns an `Option<u32>` representing its unique key of the movie title. The
    /// return value will be the Some variant holding the `u32` that is the unique key for the given movie if it exists in the Trie,
    /// otherwise None will be returned.
    pub fn delete<T: Into<Vec<char>>>(&mut self, title: T) -> Option<u32> {
        let cur_root = Rc::clone(&self.root);
        let title = title.into();
        let (key, _) = self.delete_helper(&title, cur_root, 0);
        key
    }
}

/// A data structure for managing movies like a database.
/// Uses a `MovieTrie` for efficient lookups by title, as well finding results similar to a given search query.
/// Uses a `HashMap` for mapping a movies id to its current location in the data base i.e file.
pub struct MovieCollection {
    trie: MovieTrie,
    movie_map: HashMap<u32, (Movie, u64)>,
    cur_file: Option<BufReader<File>>,
}

impl MovieCollection {
    pub fn new() -> Self {
        MovieCollection {
            trie: MovieTrie::new(),
            movie_map: HashMap::new(),
            cur_file: None,
        }
    }

    pub fn load<P: AsRef<Path>>(&mut self, path: P) -> Result<(), DbError> {
        // TODO: Check if we already have an open file
        self.cur_file = if let Ok(f) = OpenOptions::new().read(true).append(true).create(true).open(path) {
            Some(BufReader::new(f))
        } else {
            return Err(DbError::LoadError);
        };

        // load data from cur file into `self.trie` and `self.movie_map`
        // We know if we have reached this point a file has been loaded
        let f = self.cur_file.as_mut().unwrap();

        // Buffer for reading length tag for each record
        let mut cur_length_buf = [0; 12];
        // For creating the
        let mut cur_pos = SeekFrom::Start(0);

        if let Ok(_) = f.read_exact(&mut cur_length_buf) {
            // Get length of encoded data id, title_len, description_len, image_len
            // TODO: Fix this!
            let (id, title_len, description_len, image_len) = MovieCollection::decode_length(&cur_length_buf);

            // Read the bytes of the encoded movie
            let mut title = Vec::with_capacity(title_len as usize);
            let mut description = Vec::with_capacity(description_len as usize);
            let mut image = Vec::with_capacity(image_len as usize);

            if let Err(_) = f.read_exact(title.as_mut_slice()) {
                return Err(DbError::LoadError);
            }
            if let Err(_) = f.read_exact(description.as_mut_slice()) {
                return Err(DbError::LoadError);
            }
            if let Err(_) = f.read_exact(image.as_mut_slice()) {
                return Err(DbError::LoadError);
            }

        }

        Ok(())
    }

    fn decode_length(bytes: &[u8; 12]) -> (u32, u32, u32) {
        let (mut title_len, mut description_len, mut image_len) = (0, 0, 0);
        for i in 0..4 {
            title_len ^= (bytes[i] as u32) << (i * 8);
        }
        for i in 4..8 {
            description_len ^= (bytes[i] as u32) << ((i % 4) * 8);
        }
        for i in 8..12 {
            image_len ^= (bytes[i] as u32) << ((i % 4) * 8);
        }
        (title_len, description_len, image_len)
    }

    fn encode_length(title_len: u32, description_len: u32, image_len: u32) -> [u8; 12] {
        let mut bytes = [0_u8; 12];
        for i in 0..4 {
            bytes[i] = ((title_len >> (i * 8)) as u8) & 0xff;
        }
        for i in 4..8 {
            bytes[i] = ((description_len >> ((i % 4) * 8)) as u8) & 0xff;
        }
        for i in 8..12 {
            bytes[i] = ((image_len >> ((i % 4) * 8)) as u8) & 0xff;
        }
        bytes
    }
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

    #[test]
    fn test_encode_decode_length() {
        let title_len = 23697_u32;
        let description_len = 3292_u32;
        let image_len = 738_u32;

        let encoding = MovieCollection::encode_length(title_len, description_len, image_len);
        println!("{:?}", encoding);

        let (title_len_decoded, description_len_decoded, image_len_decoded) = MovieCollection::decode_length(&encoding);
        println!("{:?}", title_len_decoded);
        println!("{:?}", description_len_decoded);
        println!("{:?}", image_len_decoded);

        assert_eq!(title_len, title_len_decoded);
        assert_eq!(description_len, description_len_decoded);
        assert_eq!(image_len, image_len_decoded);
    }
}
