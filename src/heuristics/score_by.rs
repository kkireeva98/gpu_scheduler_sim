
use crate::types::*;

// Used AI to help generate the Iterator trait

// Trait to extend Iterator with scoring methods
pub trait ScoreBy: Iterator {
    fn score_by_max<S>(self, score_func: S) -> Option<Self::Item>
    where
        Self: Sized,
        S: Fn(Self::Item) -> (Self::Item, SCORE);
        
    fn score_by_min<S>(self, score_func: S) -> Option<Self::Item>
    where
        Self: Sized,
        S: Fn(Self::Item) -> (Self::Item, SCORE);
}

// Implement ScoreBy for all types that implement Iterator
impl<T: Iterator> ScoreBy for T {
    fn score_by_max<S>(self, score_func: S) -> Option<Self::Item>
    where
        Self: Sized,
        S: Fn(Self::Item) -> (Self::Item, SCORE),
    {
        self.map(score_func)
            .max_by_key(|(_, score)| *score)
            .map(|(item, _)| item)
    }
    
    fn score_by_min<S>(self, score_func: S) -> Option<Self::Item>
    where
        Self: Sized,
        S: Fn(Self::Item) -> (Self::Item, SCORE),
    {
        self.map(score_func)
            .min_by_key(|(_, score)| *score)
            .map(|(item, _)| item)
    }
}