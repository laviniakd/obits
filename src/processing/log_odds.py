from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import nltk

cv_tokenizer = CountVectorizer().build_tokenizer()

# function def: log odds ratio to find distinctive words between splits in data
def log_odds_ratio(counts_a, counts_b, alpha=0.01):
    """
    Compute the log odds ratio with informative Dirichlet prior.
    
    Parameters:
    counts_a (np.array): Word counts for group A.
    counts_b (np.array): Word counts for group B.
    alpha (float): Prior count for each word.
    
    Returns:
    np.array: Log odds ratios for each word.
    """
    # Total counts
    n_a = np.sum(counts_a)
    n_b = np.sum(counts_b)
    
    # Add prior
    counts_a += alpha
    counts_b += alpha
    n_a += alpha * len(counts_a)
    n_b += alpha * len(counts_b)
    
    # Compute log odds ratio
    log_odds = np.log((counts_a / (n_a - counts_a)) / (counts_b / (n_b - counts_b)))
    
    return log_odds


def find_distinctive_words(corpus_a, corpus_b, top_n=10, log_odds_alpha=0.01, filter_stopwords_etc=False):
    """
    Find the most distinctive words for each label in the corpus.
    
    Parameters:
    corpus_a (list of str): List of documents for group A.
    corpus_b (list of str): List of documents for group B.
    top_n (int): Number of top distinctive words to return for each label.
    
    Returns:
    dict: Dictionary with labels as keys and lists of top distinctive words as values.
    """
    vectorizer = CountVectorizer()
    # create list of words that only appear in one corpus
    words_a = set()
    words_b = set()

    print("Building vocabulary...")

    for doc in corpus_a:
        words_a.update(cv_tokenizer(doc))
    for doc in corpus_b:
        words_b.update(cv_tokenizer(doc))

    unique_words_a = words_a - words_b
    unique_words_b = words_b - words_a

    if filter_stopwords_etc:
        stopwords = set(nltk.corpus.stopwords.words('english'))
        # also numbers: 1990-2025
        stopwords.update([str(i) for i in range(1990, 2026)])
        stopwords.update(unique_words_a)
        stopwords.update(unique_words_b)
        # Apply filtering to remove stopwords and other unwanted tokens
        vectorizer = CountVectorizer(stop_words=list(stopwords))
        
    X = vectorizer.fit_transform(corpus_a + corpus_b)
    feature_names = np.array(vectorizer.get_feature_names_out())

    print("Calculating distinctive words...")
    
    distinctive_words = {}
    labels = np.array([0] * len(corpus_a) + [1] * len(corpus_b))

    counts_a = np.sum(X[labels == 0].toarray(), axis=0).astype(np.float64)
    counts_b = np.sum(X[labels == 1].toarray(), axis=0).astype(np.float64)

    log_odds = log_odds_ratio(counts_a, counts_b, alpha=log_odds_alpha)
        
    distinctive_words['corpus_a'] = feature_names[np.argsort(log_odds)[-top_n:]][::-1]
    distinctive_words['corpus_a_values'] = log_odds[np.argsort(log_odds)[-top_n:]][::-1]
    distinctive_words['corpus_b'] = feature_names[np.argsort(log_odds)[:top_n]][::-1]
    distinctive_words['corpus_b_values'] = log_odds[np.argsort(log_odds)[:top_n]][::-1]
    
    return distinctive_words
