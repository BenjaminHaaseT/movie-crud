use atomic_wait::{wait, wake_all, wake_one};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::rc::Rc;
use std::string::FromUtf8Error;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::Arc;

pub mod prelude {
    pub use super::*;
}

/// The `movie_db` error type
#[derive(Debug)]
pub enum DbError {
    AlreadyExists(String),
    FileDoesNotExist(String),
    NoFileLoaded,
    LoadError,
    ReadError(&'static str),
    CreationError,
    CollectionFull,
    WriteError,
    RecordDoesNotExist(u32),
}

impl Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::AlreadyExists(s) => write!(f, "Movie {s} already exists."),
            DbError::FileDoesNotExist(s) => write!(f, "File {s} does not exist."),
            DbError::LoadError => write!(f, "Error loading file."),
            DbError::ReadError(s) => write!(f, "Error reading {s} from file."),
            DbError::CreationError => write!(f, "Error creating record."),
            DbError::NoFileLoaded => write!(f, "No file has been loaded, unable to write record."),
            DbError::CollectionFull => write!(
                f,
                "Collection is full, unable to write an additional record"
            ),
            DbError::WriteError => write!(f, "Error writing data."),
            DbError::RecordDoesNotExist(id) => write!(f, "Record with id: {id} does not exist"),
        }
    }
}

/// A struct that represents a single movie.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Method for building a `Movie` from `Vec`s of `u8`s
    pub fn from_bytes(
        title: Vec<u8>,
        rating: u8,
        description: Vec<u8>,
        image: Vec<u8>,
    ) -> Result<Self, FromUtf8Error> {
        let title = String::from_utf8(title)?;
        let description = String::from_utf8(description)?;
        let image = String::from_utf8(image)?;
        Ok(Movie::new(title, rating, description, image))
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
    /// The method is able to fail, since it is considered an error to enter the same movie twice into the database
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

/// Struct for implementing a Trie that can be safely shared between threads
#[derive(Debug)]
struct ArcMovieTrieNode {
    /// Contains all the characters in the current node mapping them to their respective `TrieNode`s if applicable.
    children: HashMap<char, Arc<UnsafeCell<ArcMovieTrieNode>>>,
    /// A key that represents the possibility that this node terminates a given string, if `self.key` is the `Some` variant
    /// then the `u32` it holds represents the key for the given movie.
    key: Option<u32>,
}

impl ArcMovieTrieNode {
    pub fn new() -> Self {
        ArcMovieTrieNode {
            children: HashMap::new(),
            key: None,
        }
    }
}

/// A struct that gives a memory safe interface for a MovieTrie that can be shared between threads.
/// This struct is an implementation detail as part of a `DbLock`. The `DbLock` is what guarantees thread/memory safety when this struct is employed.
#[derive(Debug)]
struct ArcMovieTrie {
    root: Arc<UnsafeCell<ArcMovieTrieNode>>,
}

impl ArcMovieTrie {
    /// Associated function for creating a new `ArcMovieTrie`
    fn new() -> Self {
        ArcMovieTrie {
            root: Arc::new(UnsafeCell::new(ArcMovieTrieNode::new())),
        }
    }

    /// Checks if `title` is contained in the trie.
    /// Returns an `Option<u32>`, None if `title` is not contained within the trie, otherwise the Some
    /// variant will be returned containing the `u32` that represents that titles unique id.
    pub fn contains<T: Into<Vec<char>>>(&self, title: T) -> Option<u32> {
        let title = title.into();
        let mut cur_root = Arc::clone(&self.root);
        let mut end_of_str = true;
        for c in title {
            let next_node = if let Some(neo) = unsafe { (&*cur_root.get()).children.get(&c) } {
                Arc::clone(neo)
            } else {
                end_of_str = false;
                break;
            };
            cur_root = next_node;
        }
        if end_of_str {
            unsafe {
                return (&*cur_root.get()).key;
            }
        }
        None
    }

    /// Helper method for `find_by_prefix`, is a private implementation detail. Populates the result vector.
    fn find_by_prefix_dfs(&self, node: Arc<UnsafeCell<ArcMovieTrieNode>>, result: &mut Vec<u32>) {
        if let Some(key) = unsafe { (*node.get()).key.as_ref() } {
            result.push(*key)
        }
        let node_children = unsafe { &(*node.get()).children };
        for (_, child) in node_children {
            self.find_by_prefix_dfs(Arc::clone(child), result);
        }
    }

    /// Finds all movies that share a common prefix. Returns a `Vec<u32>` of the movies ids, empty if no movie has the given `prefix`.
    pub fn find_by_prefix<T: Into<Vec<char>>>(&self, prefix: T) -> Vec<u32> {
        let prefix = prefix.into();
        let mut cur_root = Arc::clone(&self.root);
        for c in prefix {
            let next_node = if let Some(neo) = unsafe { (&*cur_root.get()).children.get(&c) } {
                Arc::clone(neo)
            } else {
                return vec![]
            };
            cur_root = next_node;
        }
        // Proceed with dfs from cur_root
        let mut result = vec![];
        self.find_by_prefix_dfs(cur_root, &mut result);
        result

    }

    /// Inserts a new `title` into the trie with a given `key` as it is id.
    /// Note the method does not ensure that the `key` is unique, that must be ensured by the
    /// user if that property is desired. The method can potentially fail, if `title` is already
    /// in the trie. Returns a `Result<(), DbError>`.
    pub fn insert<T: Into<Vec<char>>>(&mut self, title: T, key: u32) -> Result<(), DbError> {
        let title = title.into();
        let movie_title_display = String::from_iter(title.iter());
        let mut cur_root = Arc::clone(&self.root);
        for c in title {
            let next_node = if unsafe { (&*cur_root.get()).children.contains_key(&c) } {
                Arc::clone(unsafe { &(&*cur_root.get()).children[&c] })
            } else {
                let neo = Arc::new(UnsafeCell::new(ArcMovieTrieNode::new()));
                unsafe {
                    (*cur_root.get()).children.insert(c, neo.clone());
                }
                neo
            };
            cur_root = next_node;
        }
        // Check that we haven't already added to the trie
        if unsafe { (&*cur_root.get()).key.is_some() } {
            return Err(DbError::AlreadyExists(movie_title_display));
        }
        // Add `key` to the current root
        unsafe {
            (*cur_root.get()).key = Some(key);
        }
        Ok(())
    }

    /// An implementation detail of `delete`
    fn delete_helper(
        &mut self,
        title: &Vec<char>,
        i: usize,
        cur_root: Arc<UnsafeCell<ArcMovieTrieNode>>,
    ) -> (Option<u32>, bool) {
        if i == title.len() {
            match unsafe { ((*cur_root.get()).key, (*cur_root.get()).children.is_empty()) } {
                (Some(k), true) => return (Some(k), true),
                (Some(k), false) => return (Some(k), false),
                (_, _) => return (None, false),
            }
        } else if unsafe { (*cur_root.get()).children.contains_key(&title[i]) } {
            let next_node = Arc::clone(unsafe { &(*cur_root.get()).children[&title[i]] });
            let (key, flag) = self.delete_helper(title, i + 1, next_node);
            if flag {
                unsafe {
                    (*cur_root.get()).children.remove(&title[i]);
                }
            }
            return (
                key,
                if unsafe { (*cur_root.get()).children.is_empty() } {
                    true
                } else {
                    false
                },
            );
        }
        (None, false)
    }

    /// Delete a movie from the trie. The function returns an `Option<u32>` representing its unique key of the movie title. The
    /// return value will be the Some variant holding the `u32` that is the unique key for the given movie if it exists in the Trie,
    /// otherwise None will be returned.
    pub fn delete<T: Into<Vec<char>>>(&mut self, title: T) -> Option<u32> {
        let title = title.into();
        let mut cur_root = Arc::clone(&self.root);
        self.delete_helper(&title, 0, cur_root).0
    }
}

unsafe impl Send for ArcMovieTrie {}
unsafe impl Sync for ArcMovieTrie {}

/// A thread safe `MovieCollection` implementation. A `ArcMovieCollection` is an implementation detail of a `DbLock`.
/// The object itself is only accessible through a `DbLock`. It is the `DbLock` that guarantees thread/memory safety.
pub struct ArcMovieCollection {
    trie: ArcMovieTrie,
    movie_map: HashMap<u32, (Movie, u64)>,
    write_on_drop: bool,
    cur_file: Option<File>,
    cur_id: u32,
}

impl ArcMovieCollection {
    fn new() -> Self {
        Self {
            trie: ArcMovieTrie::new(),
            movie_map: HashMap::new(),
            write_on_drop: false,
            cur_file: None,
            cur_id: 0,
        }
    }

    /// Helper method for writing movie information to the file
    fn write_record(id: &u32, movie: &Movie, f: &mut impl Write) -> Result<(), DbError> {
        let title_bytes = movie.title.as_bytes();
        let description_bytes = movie.description.as_bytes();
        let image_bytes = movie.image.as_bytes();
        let rating = movie.rating;
        let tag = ArcMovieCollection::encode_tag(
            *id,
            title_bytes.len() as u32,
            description_bytes.len() as u32,
            image_bytes.len() as u32,
            rating
        );
        let movie_bytes = tag
            .iter()
            .map(|b| *b)
            .chain(title_bytes.iter().map(|b| *b))
            .chain(description_bytes.iter().map(|b| *b))
            .chain(image_bytes.iter().map(|b| *b))
            .collect::<Vec<u8>>();
        if let Err(_) = f.write(tag.as_slice()) {
            return Err(DbError::WriteError);
        }
        Ok(())
    }

    pub fn write_to_file_flush(&mut self) -> Result<(), DbError> {
        let mut f = if let Some(f) = self.cur_file.take() {
            BufWriter::new(f)
        } else {
            return Err(DbError::NoFileLoaded);
        };
        // Write data to file
        for (id, (movie, _)) in &self.movie_map {
            // let title_bytes = movie.title.as_bytes();
            // let description_bytes = movie.description.as_bytes();
            // let image_bytes = movie.image.as_bytes();
            // let rating = movie.rating;
            // let tag = ArcMovieCollection::encode_tag(
            //     *id,
            //     title_bytes.len() as u32,
            //     description_bytes.len() as u32,
            //     image_bytes.len() as u32,
            //     rating,
            // );
            // let movie_bytes = tag
            //     .iter()
            //     .map(|b| *b)
            //     .chain(title_bytes.iter().map(|b| *b))
            //     .chain(description_bytes.iter().map(|b| *b))
            //     .chain(image_bytes.iter().map(|b| *b))
            //     .collect::<Vec<u8>>();
            // if let Err(_) = f.write(&movie_bytes) {
            //     // Save the file handle
            //     self.cur_file = Some(f.into_inner().unwrap());
            //     return Err(DbError::WriteError);
            // }
            ArcMovieCollection::write_record(id, movie, &mut f)?;
        }
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(&mut self, path: P) -> Result<(), DbError> {
        // First check if we have a current file open
        if self.cur_file.is_some() {
            self.write_to_file_flush()?;
        }
        // Read the file from the provided path
        let mut f = if let Ok(f) = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(path)
        {
            BufReader::new(f)
        } else {
            eprintln!("error loading file");
            return Err(DbError::LoadError);
        };

        // For keeping track of where we are in the file
        let mut tag_buf = [0_u8; 17];
        let mut cur_pos = SeekFrom::Start(0);
        let mut cur_pos_res = f.seek(cur_pos);

        while let Ok(_) = f.read_exact(&mut tag_buf) {
            // Read the tag for the current record
            let (id, title_len, description_len, image_len, rating) =
                ArcMovieCollection::decode_tag(&tag_buf);
            let mut title_bytes = vec![0_u8; title_len as usize];
            let mut description_bytes = vec![0_u8; description_len as usize];
            let mut image_bytes = vec![0_u8; image_len as usize];
            // Read raw bytes into vectors
            if let Err(_e) = f.read_exact(title_bytes.as_mut_slice()) {
                return Err(DbError::ReadError("title"));
            }
            if let Err(_e) = f.read_exact(description_bytes.as_mut_slice()) {
                return Err(DbError::ReadError("description"));
            }
            if let Err(_e) = f.read_exact(image_bytes.as_mut_slice()) {
                return Err(DbError::ReadError("image"));
            }
            // Bytes read successfully, now create movie
            let movie = if let Ok(m) =
                Movie::from_bytes(title_bytes, rating, description_bytes, image_bytes)
            {
                m
            } else {
                eprintln!("failed to create movie");
                return Err(DbError::CreationError);
            };
            if let Ok(pos) = cur_pos_res {
                // Reset `self.cur_id` to max to ensure we always have `self.cur_id`
                // at the next possible valid id
                self.cur_id = u32::max(self.cur_id, id);
                self.add_record_from_file(pos, id, movie)?;
            } else {
                eprintln!("error getting current location in file");
                return Err(DbError::LoadError);
            }
            // Move file cursor to next position
            cur_pos = SeekFrom::Current(0);
            cur_pos_res = f.seek(cur_pos);
            // Reset tag buffer
            tag_buf = [0_u8; 17];
        }
        // Set the current file of the collection
        self.cur_file = Some(f.into_inner());
        Ok(())
    }

    /// Method for finding if a movie exists in the current collection. Returns an `Option<u32>`,
    /// None if the record does not exist, otherwise Some.
    pub fn find(&self, title: String) -> Option<u32> {
        let title = title.chars().collect::<Vec<char>>();
        self.trie.contains(title)
    }

    /// Method for finding movies that share a common prefix. Returns a `Vec<Movie>` of cloned `Movie`s.
    pub fn find_by_prefix(&self, prefix: String) -> Vec<Movie> {
        let prefix = prefix.chars().collect::<Vec<char>>();
        self.trie.find_by_prefix(prefix)
            .into_iter()
            .filter_map(|id| {
                if let Some((m,_)) = self.movie_map.get(&id) {
                    Some(m.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<Movie>>()
    }

    /// Method for getting a movie based on an id
    pub fn get_movie(&self, id: u32) -> Option<&Movie> {
        if let Some((movie, _)) = self.movie_map.get(&id) {
            Some(movie)
        } else {
            None
        }
    }

    /// Method for getting all movies from the collection
    pub fn get_all_movies(&self) -> Vec<&Movie> {
        self.movie_map.iter().map(|(_,(movie, _))| movie).collect::<Vec<&Movie>>()
    }

    /// Method for deleting a movie from the collection. Returns a `Result<(), DbError>`, note it is considered an error to try and
    /// delete a movie that is not currently in the collection.
    pub fn delete(&mut self, id: u32) -> Result<(), DbError> {
        if !self.movie_map.contains_key(&id) {
            return Err(DbError::RecordDoesNotExist(id));
        }
        let (movie, _) = self.movie_map.remove(&id).unwrap();
        let title = movie.title.chars().collect::<Vec<char>>();
        self.trie.delete(title);
        self.write_on_drop = true;
        Ok(())
    }

    /// Updates a movie with `id` with `new_movie`
    pub fn update(&mut self, id: u32, new_movie: Movie) -> Result<(), DbError> {
        if let Some((old_movie, _)) = self.movie_map.get_mut(&id) {
            if new_movie.title != old_movie.title {
                old_movie.title = new_movie.title;
            }
            if new_movie.description != old_movie.description {
                old_movie.description = new_movie.description;
            }
            if new_movie.image != old_movie.image {
                old_movie.image = new_movie.image;
            }
            if new_movie.rating != old_movie.rating {
                old_movie.rating = new_movie.rating;
            }
            self.write_on_drop = true;
            return Ok(());
        }
        Err(DbError::RecordDoesNotExist(id))
    }

    /// Method for adding an arbitrary record to the collection. Takes a `Movie` and returns a result
    /// Ok(()) if the movie was added successfully, and Err if the movie was already contained in the collection.
    pub fn add_record(&mut self, movie: Movie) -> Result<(), DbError> {
        // Get a reference to the file handle currently loaded
        let mut f = if let Some(f) = self.cur_file.as_mut() {
            f
        } else {
            return Err(DbError::NoFileLoaded);
        };
        // Get byte position in current file from the end
        let byte_pos = if let Ok(b) = f.seek(SeekFrom::End(0)) {
            b
        } else {
            return Err(DbError::LoadError);
        };
        // Increment id for new record
        self.cur_id += 1;
        // Write new record to file
        let title_bytes = movie.title.as_bytes();
        let description_bytes = movie.description.as_bytes();
        let image_bytes = movie.image.as_bytes();
        let rating = movie.rating;
        let tag = MovieCollection::encode_tag(
            self.cur_id,
            title_bytes.len() as u32,
            description_bytes.len() as u32,
            image_bytes.len() as u32,
            rating,
        );
        let movie_bytes = tag
            .iter()
            .map(|b| *b)
            .chain(title_bytes.iter().map(|b| *b))
            .chain(description_bytes.iter().map(|b| *b))
            .chain(image_bytes.iter().map(|b| *b))
            .collect::<Vec<u8>>();
        if let Err(_) = f.write(movie_bytes.as_slice()) {
            return Err(DbError::WriteError);
        }
        self.add_record_from_file(byte_pos, self.cur_id, movie)
    }

    /// Private implementation detail, essentially a helper function for writing records to the collection
    fn add_record_from_file(&mut self, pos: u64, id: u32, movie: Movie) -> Result<(), DbError> {
        let title = movie.title.chars().collect::<Vec<char>>();
        // Add movie to trie
        self.trie.insert(title, id)?;
        self.movie_map.insert(id, (movie, pos));
        Ok(())
    }

    /// Private helper method to decode the tag from a slice of bytes
    fn decode_tag(bytes: &[u8; 17]) -> (u32, u32, u32, u32, u8) {
        let (mut id, mut title_len, mut description_len, mut image_len) = (0, 0, 0, 0);
        for i in 0..4 {
            id ^= (bytes[i] as u32) << (i * 8);
        }
        for i in 4..8 {
            title_len ^= (bytes[i] as u32) << ((i % 4) * 8);
        }
        for i in 8..12 {
            description_len ^= (bytes[i] as u32) << ((i % 4) * 8);
        }
        for i in 12..16 {
            image_len ^= (bytes[i] as u32) << ((i % 4) * 8);
        }
        let rating = bytes[16];
        (id, title_len, description_len, image_len, rating)
    }

    /// Private helper method to encode a tag as a slice of bytes from given movie attributes
    fn encode_tag(
        id: u32,
        title_len: u32,
        description_len: u32,
        image_len: u32,
        rating: u8,
    ) -> [u8; 17] {
        let mut bytes = [0_u8; 17];
        for i in 0..4 {
            bytes[i] = ((id >> (i * 8)) & 0xff) as u8;
        }
        for i in 4..8 {
            bytes[i] = ((title_len >> ((i % 4) * 8)) & 0xff) as u8;
        }
        for i in 8..12 {
            bytes[i] = ((description_len >> ((i % 4) * 8)) & 0xff) as u8;
        }
        for i in 12..16 {
            bytes[i] = ((image_len >> ((i % 4) * 8)) & 0xff) as u8;
        }
        bytes[16] = rating;
        bytes
    }
}

unsafe impl Send for ArcMovieCollection {}

impl Drop for ArcMovieCollection {
    fn drop(&mut self) {
        // Write all changes to file
        if self.write_on_drop {
            if self.cur_file.is_some() {
                let _ = self.write_to_file_flush();
            } else {
                // Should never happen
                panic!("file should be loaded for write_on_drop")
            }
        }

    }
}

/// A lock for interacting with a `ArcMovieCollection` in a thread safe way i.e. provides the
/// thread/memory safety guarantees for the `ArcMovieCollection` struct.
pub struct DbLock {
    /// Holds the collection of movie data.
    collection: UnsafeCell<ArcMovieCollection>,
    /// A flag, 0: unlocked,  1: locked
    state: AtomicU32,
}

/// Essentially a 3 state mutex that allows efficient and thread safe access to a `ArcMovieCollection` struct.
impl DbLock {
    /// Associated method for creating a new `DbLock`
    pub fn new() -> Self {
        Self {
            collection: UnsafeCell::new(ArcMovieCollection::new()),
            /// 0: unlocked
            /// 1: locked with no waiting threads
            /// 2: locked with waiting threads
            state: AtomicU32::new(0),
        }
    }
    /// Associated method creating a `DbLock` with a preloaded file. The method can potentially fail,
    /// and therefore will return an error on failure.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, DbError> {
        let mut collection = ArcMovieCollection::new();
        collection.load(path)?;
        Ok(Self {
            collection: UnsafeCell::new(collection),
            state: AtomicU32::new(0),
        })
    }

    /// Locks the `DbLock` and returns the `DbLockGuard` for interacting with the `ArcMovieCollection`.
    pub fn lock(&self) -> DbLockGuard {
        if self.state.compare_exchange(0, 1, Acquire, Relaxed).is_err() {
            // Change the state to 2 to signal locked with waiting threads
            loop {
                match self.state.swap(2, Acquire) {
                    0 => break,
                    s => {
                        wait(&self.state, s);
                    }
                }
            }
        }
        DbLockGuard { dblock: self }
    }
}

unsafe impl Send for DbLock {}
unsafe impl Sync for DbLock {}

pub struct DbLockGuard<'a> {
    dblock: &'a DbLock,
}

impl Deref for DbLockGuard<'_> {
    type Target = ArcMovieCollection;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.dblock.collection.get() }
    }
}

impl DerefMut for DbLockGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.dblock.collection.get() }
    }
}

impl Drop for DbLockGuard<'_> {
    fn drop(&mut self) {
        // check if there are any waiters
        if self.dblock.state.swap(0, Release) == 2 {
            wake_one(&self.dblock.state);
        }
    }
}

/// A data structure for managing movies like a database.
/// Uses a `MovieTrie` for efficient lookups by title, as well finding results similar to a given search query.
/// Uses a `HashMap` for mapping a movies id to its current location in the data base i.e file.
pub struct MovieCollection {
    trie: MovieTrie,
    movie_map: HashMap<u32, (Movie, u64)>,
    cur_file: Option<File>,
    cur_id: u32,
}

impl MovieCollection {
    pub fn new() -> Self {
        MovieCollection {
            trie: MovieTrie::new(),
            movie_map: HashMap::new(),
            cur_file: None,
            cur_id: 0,
        }
    }

    /// Public method to write all data contained in `self` to the current file.
    /// Returns an error if there is no file loaded. After writing to the file, the `MovieCollection
    /// will no longer contain any data, and will no longer hold a reference to a file.
    /// This call will overwrite the contents of the file contained in `self.cur_file`, all data held
    /// currently by the `MovieCollection` will be removed as well.
    pub fn write_to_file_flush(&mut self) -> Result<(), DbError> {
        let mut cur_file = if let Some(f) = self.cur_file.take() {
            BufWriter::new(f)
        } else {
            return Err(DbError::NoFileLoaded);
        };

        for (id, (movie, _)) in &self.movie_map {
            let title = movie.title.as_bytes();
            let description = movie.description.as_bytes();
            let image = movie.image.as_bytes();
            let encoded_tag = MovieCollection::encode_tag(
                *id,
                title.len() as u32,
                description.len() as u32,
                image.len() as u32,
                movie.rating,
            );

            // Collect all bytes into one vector to write to the file
            let movie_bytes = encoded_tag
                .iter()
                .map(|b| *b)
                .chain(title.iter().map(|b| *b))
                .chain(description.iter().map(|b| *b))
                .chain(image.iter().map(|b| *b))
                .collect::<Vec<u8>>();
            if let Err(_) = cur_file.write(movie_bytes.as_slice()) {
                // Reassign file in order to not lose potential data
                // We know cur_file contains a valid `File` struct, so it is safe to unwrap
                self.cur_file = Some(cur_file.into_inner().unwrap());
                return Err(DbError::WriteError);
            }
        }

        // We have successfully written all the data to the file,
        // remove data from `self.trie` and `self.movie_map`
        self.trie = MovieTrie::new();
        self.movie_map = HashMap::new();
        self.cur_id = 0;
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(&mut self, path: P) -> Result<(), DbError> {
        // Check if we already have a current file open, if so write and flush the contents of the current movie collection.
        if self.cur_file.is_some() {
            self.write_to_file_flush()?;
        }
        let mut f = if let Ok(f) = OpenOptions::new()
            .read(true)
            .append(true)
            .write(true)
            .create(true)
            .open(path)
        {
            BufReader::new(f)
        } else {
            println!("File not loaded");
            return Err(DbError::LoadError);
        };

        // load data from cur file into `self.trie` and `self.movie_map`
        // We know if we have reached this point a file has been loaded
        // Buffer for reading the tag of each record
        let mut cur_tag_buf = [0; 17];

        // For keeping track of where each record is located in the file
        let mut cur_pos = SeekFrom::Start(0);
        let mut cur_pos_res = f.seek(cur_pos);

        while let Ok(_) = f.read_exact(&mut cur_tag_buf) {
            // Read tag from the buffer
            let (id, title_len, description_len, image_len, rating) =
                MovieCollection::decode_tag(&cur_tag_buf);

            // Allocate vectors for bytes to be read from file
            let mut title_bytes = vec![0; title_len as usize];
            let mut description_bytes = vec![0; description_len as usize];
            let mut image_bytes = vec![0; image_len as usize];

            // Read the bytes from file
            if let Err(e) = f.read_exact(title_bytes.as_mut_slice()) {
                return Err(DbError::ReadError("title"));
            }
            if let Err(e) = f.read(description_bytes.as_mut_slice()) {
                return Err(DbError::ReadError("description"));
            }
            if let Err(e) = f.read(image_bytes.as_mut_slice()) {
                return Err(DbError::ReadError("image link"));
            }
            // Construct movie object
            let movie = if let Ok(m) =
                Movie::from_bytes(title_bytes, rating, description_bytes, image_bytes)
            {
                m
            } else {
                println!("Failed to create movie");
                return Err(DbError::CreationError);
            };
            // Ensure we have the cursor position of the current record
            if let Ok(pos) = cur_pos_res {
                // Reset `self.cur_id` to max to ensure we always have `self.cur_id`
                // at the next possible valid id
                self.cur_id = u32::max(self.cur_id, id);
                self.add_record_from_file(pos, id, movie)?;
            } else {
                println!("Failed to get cursor position");
                return Err(DbError::LoadError);
            }
            // Move current position in file for next record if any
            cur_pos = SeekFrom::Current(0);
            cur_pos_res = f.seek(cur_pos);
            // Reset cur_tag_buf for next record
            cur_tag_buf = [0; 17];
        }

        // Set `self.cur_file` to f
        self.cur_file = Some(f.into_inner());
        Ok(())
    }

    /// Searches for a record in the collection. Returns an `Option`, the return value will
    /// be the None variant if the record does not exist, otherwise it will be the some variant with
    /// the record's id represented as a `u32`.
    pub fn find(&self, title: String) -> Option<u32> {
        let title = title.chars().collect::<Vec<char>>();
        self.trie.contains(title)
    }

    /// Deletes a record from the collection
    pub fn delete(&mut self, id: u32) -> Result<(), DbError> {
        if !self.movie_map.contains_key(&id) {
            return Err(DbError::RecordDoesNotExist(id));
        }
        // We know the record exists at this point i.e safe to unwrap
        let (movie, _) = self.movie_map.remove(&id).unwrap();
        let title = movie.title.chars().collect::<Vec<char>>();
        self.trie.delete(title);
        Ok(())
    }

    /// Adds a movie to the `MovieCollection`. Takes a `Movie` struct as a parameter the movie to add to the collection.
    /// Returns a `Result<(), `DbError`>.
    pub fn add_record(&mut self, movie: Movie) -> Result<(), DbError> {
        if self.cur_file.is_none() {
            return Err(DbError::NoFileLoaded);
        }

        let cur_file = self.cur_file.as_mut().unwrap();
        let cur_pos_bytes = if let Ok(b) = cur_file.seek(SeekFrom::End(0)) {
            b
        } else {
            return Err(DbError::LoadError);
        };
        if self.cur_id == u32::MAX {
            return Err(DbError::CollectionFull);
        }
        self.cur_id += 1;
        // Add record to collection
        self.add_record_from_file(cur_pos_bytes, self.cur_id, movie)
    }

    /// Private helper method, adds a record from a file.
    fn add_record_from_file(&mut self, pos: u64, id: u32, movie: Movie) -> Result<(), DbError> {
        // First add movie title with `id` as `key` into `self.trie`
        let title = movie.title.chars().collect::<Vec<char>>();
        self.trie.insert(title, id)?;
        self.movie_map.insert(id, (movie, pos));
        Ok(())
    }

    /// Private helper method to decode the tag from a slice of bytes
    fn decode_tag(bytes: &[u8; 17]) -> (u32, u32, u32, u32, u8) {
        let (mut id, mut title_len, mut description_len, mut image_len) = (0, 0, 0, 0);
        for i in 0..4 {
            id ^= (bytes[i] as u32) << (i * 8);
        }
        for i in 4..8 {
            title_len ^= (bytes[i] as u32) << ((i % 4) * 8);
        }
        for i in 8..12 {
            description_len ^= (bytes[i] as u32) << ((i % 4) * 8);
        }
        for i in 12..16 {
            image_len ^= (bytes[i] as u32) << ((i % 4) * 8);
        }
        let rating = bytes[16];
        (id, title_len, description_len, image_len, rating)
    }

    /// Private helper method to encode a tag as a slice of bytes from given movie attributes
    fn encode_tag(
        id: u32,
        title_len: u32,
        description_len: u32,
        image_len: u32,
        rating: u8,
    ) -> [u8; 17] {
        let mut bytes = [0_u8; 17];
        for i in 0..4 {
            bytes[i] = ((id >> (i * 8)) & 0xff) as u8;
        }
        for i in 4..8 {
            bytes[i] = ((title_len >> ((i % 4) * 8)) & 0xff) as u8;
        }
        for i in 8..12 {
            bytes[i] = ((description_len >> ((i % 4) * 8)) & 0xff) as u8;
        }
        for i in 12..16 {
            bytes[i] = ((image_len >> ((i % 4) * 8)) & 0xff) as u8;
        }
        bytes[16] = rating;
        bytes
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;

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
        let id = 38928371;
        let title_len = 23697_u32;
        let description_len = 3292_u32;
        let image_len = 738_u32;
        let rating = 3_u8;

        let encoding =
            MovieCollection::encode_tag(id, title_len, description_len, image_len, rating);
        println!("{:?}", encoding);

        let (
            id_decoded,
            title_len_decoded,
            description_len_decoded,
            image_len_decoded,
            rating_decoded,
        ) = MovieCollection::decode_tag(&encoding);
        println!("{:?}", id_decoded);
        println!("{:?}", title_len_decoded);
        println!("{:?}", description_len_decoded);
        println!("{:?}", image_len_decoded);
        println!("{:?}", rating);

        assert_eq!(id, id_decoded);
        assert_eq!(title_len, title_len_decoded);
        assert_eq!(description_len, description_len_decoded);
        assert_eq!(image_len, image_len_decoded);
        assert_eq!(rating, rating_decoded);
    }

    #[test]
    fn test_create_collection() {
        let mut movie_collection = MovieCollection::new();
        assert!(movie_collection.load("movies.txt").is_ok());
        let movie1 = Movie::new(
            "Lord of the Rings: The Fellowship of the Ring".to_string(),
            5,
            "A brilliant movie".to_string(),
            "N/A".to_string(),
        );
        let movie2 = Movie::new(
            "Lord of the Rings: The Two Towers".to_string(),
            5,
            "Another brilliant movie".to_string(),
            "N/A".to_string(),
        );
        let movie3 = Movie::new(
            "Lord of the Rings: The Return of the King".to_string(),
            5,
            "A brilliant ending to the trilogy".to_string(),
            "N/a".to_string(),
        );
        let movie4 = Movie::new(
            "Borat".to_string(),
            5,
            "A very funny movie".to_string(),
            "N/A".to_string(),
        );
        assert!(movie_collection.add_record(movie1).is_ok());
        assert!(movie_collection.add_record(movie2).is_ok());
        assert!(movie_collection.add_record(movie3).is_ok());
        assert!(movie_collection.add_record(movie4).is_ok());
        assert!(movie_collection.write_to_file_flush().is_ok());
    }

    #[test]
    fn test_load_collection() {
        let mut movie_collection = MovieCollection::new();
        assert!(movie_collection.load("movies.txt").is_ok());

        assert!(movie_collection
            .find("Lord of the Rings: The Fellowship of the Ring".to_string())
            .is_some());
        let id = movie_collection
            .find("Lord of the Rings: The Fellowship of the Ring".to_string())
            .unwrap();
        println!("{id}");

        assert!(movie_collection.find("Borat".to_string()).is_some());
        let id = movie_collection.find("Borat".to_string()).unwrap();
        println!("{id}");
    }

    #[test]
    fn test_add_record_to_collection() {
        let mut movie_collection = MovieCollection::new();
        assert!(movie_collection.load("movies.txt").is_ok());

        let movie = Movie::new(
            "Fear and Loathing in Las Vegas".to_string(),
            5,
            "An amazing movie".to_string(),
            "N/A".to_string(),
        );
        assert!(movie_collection.add_record(movie).is_ok());

        if let Some(id) = movie_collection.find("Fear and Loathing in Las Vegas".to_string()) {
            println!("Fear and Loathing in Las Vegas has ID: {id}");
            assert!(true);
        } else {
            // Panic in this case
            assert!(false);
        }

        // Ensure we still have other data loaded in the collection
        if let Some(id) =
            movie_collection.find("Lord of the Rings: The Fellowship of the Ring".to_string())
        {
            println!("Lord of the Rings: The Fellowship of the Ring has ID: {id}");
            assert!(true);
        } else {
            assert!(false);
        }

        println!("{:?}", movie_collection.movie_map);
    }

    #[test]
    fn test_delete_record_from_collection() {
        let mut movie_collection = MovieCollection::new();
        assert!(movie_collection.load("movies.txt".to_string()).is_ok());

        assert!(movie_collection.find("Borat".to_string()).is_some());
        if let Some(id) = movie_collection.find("Borat".to_string()) {
            println!("ID: {id}");
            assert!(movie_collection.delete(id).is_ok());
        }

        assert!(movie_collection.find("Borat".to_string()).is_none());
        println!("{:?}", movie_collection.movie_map);
    }

    #[test]
    fn test_create_collection_arc() {
        let mut movie_collection = ArcMovieCollection::new();
        assert!(movie_collection.load("movies_arc.txt").is_ok());
        let movie1 = Movie::new(
            "Lord of the Rings: The Fellowship of the Ring".to_string(),
            5,
            "A brilliant movie".to_string(),
            "N/A".to_string(),
        );
        let movie2 = Movie::new(
            "Lord of the Rings: The Two Towers".to_string(),
            5,
            "Another brilliant movie".to_string(),
            "N/A".to_string(),
        );
        let movie3 = Movie::new(
            "Lord of the Rings: The Return of the King".to_string(),
            5,
            "A brilliant ending to the trilogy".to_string(),
            "N/a".to_string(),
        );
        let movie4 = Movie::new(
            "Borat".to_string(),
            5,
            "A very funny movie".to_string(),
            "N/A".to_string(),
        );
        assert!(movie_collection.add_record(movie1).is_ok());
        assert!(movie_collection.add_record(movie2).is_ok());
        assert!(movie_collection.add_record(movie3).is_ok());
        assert!(movie_collection.add_record(movie4).is_ok());
        // assert!(movie_collection.write_to_file_flush().is_ok());
    }

    #[test]
    fn test_load_collection_arc() {
        let mut movie_collection = ArcMovieCollection::new();
        assert!(movie_collection.load("movies_arc.txt").is_ok());

        assert!(movie_collection
            .find("Lord of the Rings: The Fellowship of the Ring".to_string())
            .is_some());
        let id = movie_collection
            .find("Lord of the Rings: The Fellowship of the Ring".to_string())
            .unwrap();
        println!("{id}");

        assert!(movie_collection.find("Borat".to_string()).is_some());
        let id = movie_collection.find("Borat".to_string()).unwrap();
        println!("{id}");
    }

    #[test]
    fn test_add_record_to_collection_arc() {
        let mut movie_collection = ArcMovieCollection::new();
        assert!(movie_collection.load("movies_arc.txt").is_ok());

        let movie = Movie::new(
            "Fear and Loathing in Las Vegas".to_string(),
            5,
            "An amazing movie".to_string(),
            "N/A".to_string(),
        );
        assert!(movie_collection.add_record(movie).is_ok());

        if let Some(id) = movie_collection.find("Fear and Loathing in Las Vegas".to_string()) {
            println!("Fear and Loathing in Las Vegas has ID: {id}");
            assert!(true);
        } else {
            // Panic in this case
            assert!(false);
        }

        // Ensure we still have other data loaded in the collection
        if let Some(id) =
            movie_collection.find("Lord of the Rings: The Fellowship of the Ring".to_string())
        {
            println!("Lord of the Rings: The Fellowship of the Ring has ID: {id}");
            assert!(true);
        } else {
            assert!(false);
        }

        println!("{:?}", movie_collection.movie_map);
    }

    #[test]
    fn test_delete_record_from_collection_arc() {
        let mut movie_collection = ArcMovieCollection::new();
        assert!(movie_collection.load("movies_arc.txt".to_string()).is_ok());

        assert!(movie_collection.find("Borat".to_string()).is_some());
        if let Some(id) = movie_collection.find("Borat".to_string()) {
            println!("ID: {id}");
            assert!(movie_collection.delete(id).is_ok());
        }

        assert!(movie_collection.find("Borat".to_string()).is_none());
        println!("{:?}", movie_collection.movie_map);
    }

    #[test]
    fn test_db_lock_creation() {
        let dblock = DbLock::new();
        assert!(true);
    }

    #[test]
    fn test_dblock_in_threads() {
        let dblock = DbLock::new();
        assert!(dblock.lock().load("test_movies.txt").is_ok());
        println!("Loaded test_movies.txt successfully.");

        thread::scope(|s| {
            s.spawn(|| {
                for i in 1..=1000 {
                    let movie = Movie::new(
                        format!("Avengers {i}"),
                        0,
                        String::from("A terrible movie"),
                        String::from("N/A"),
                    );
                    assert!(dblock.lock().add_record(movie).is_ok());
                }
            });
            s.spawn(|| {
                for i in 1..=1000 {
                    let movie = Movie::new(
                        format!("Disney Star Wars {i}"),
                        0,
                        String::from("A terrible movie"),
                        String::from("N/A"),
                    );
                    assert!(dblock.lock().add_record(movie).is_ok());
                }
            });
        });
    }

    #[test]
    fn test_dblock_reading_in_threads() {
        let dblock = DbLock::new();
        assert!(dblock.lock().load("test_movies.txt").is_ok());

        thread::scope(|s| {
            s.spawn(|| {
                for i in 1..=1000 {
                    assert!(dblock.lock().find(format!("Avengers {i}")).is_some());
                }
            });
            s.spawn(|| {
                for i in 1..=1000 {
                    assert!(dblock
                        .lock()
                        .find(format!("Disney Star Wars {i}"))
                        .is_some());
                }
            });
        });
    }

    #[test]
    fn test_prefix_find() {
        let mut movie_collection = ArcMovieCollection::new();
        assert!(movie_collection.load("movies_arc.txt").is_ok());

        let res = movie_collection.find_by_prefix("Lord ".to_string());
        println!("results: {:?}", res);

        let res = movie_collection.find_by_prefix("Lord of the Rings: The R".to_string());
        println!("results: {:?}", res);
    }
}
