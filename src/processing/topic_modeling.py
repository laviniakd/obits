import pandas as pd
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from src.data.load_data import load_jsons_to_dataframe
import os
import pickle as pkl
import nltk
nltk.download('stopwords')

os.makedirs('topic_model', exist_ok=True)
LDA_PATH = 'topic_model/lda_model.pkl'
VECTORIZER_PATH = 'topic_model/vectorizer.pkl'
KEYWORDS_PATH = 'topic_model/topic_keywords.txt'
N_TOPICS = 10
REMOVE_STOPWORDS_AND_NUMBERS = True

def preprocess_text(text):
    return text.lower()

def train_lda_model(documents, num_topics=N_TOPICS, max_features=500):
    processed_docs = [preprocess_text(doc) for doc in documents]
    
    vectorizer = CountVectorizer(max_features=max_features)
    doc_term_matrix = vectorizer.fit_transform(processed_docs)

    lda_model = LatentDirichletAllocation(n_components=num_topics, 
                                          max_iter=25,
                                          random_state=42)
    lda_model.fit(doc_term_matrix)
    
    return lda_model, vectorizer

def get_topic_keywords(lda_model, vectorizer, num_keywords=10):
    keywords = []
    for topic_idx, topic in enumerate(lda_model.components_):
        top_keywords_indices = topic.argsort()[-num_keywords:][::-1]
        top_keywords = [vectorizer.get_feature_names_out()[i] for i in top_keywords_indices]
        keywords.append((topic_idx, top_keywords))
    return keywords

def main():
    data = load_jsons_to_dataframe()
    data = data.dropna(subset=['text'])
    documents = data['text'].tolist()

    # remove stopwords
    if REMOVE_STOPWORDS_AND_NUMBERS:
        # remove stopwords
        stopwords = nltk.corpus.stopwords.words('english')
        stopwords = set(stopwords)
        documents = [' '.join([word for word in doc.split() if word.lower() not in stopwords]) for doc in documents]
        # remove numbers
        documents = [' '.join([word for word in doc.split() if not word.isdigit()]) for doc in documents]

    print(f"Loaded {len(documents)} documents for topic modeling.")

    print(f"Training LDA model with {N_TOPICS} topics...")
    
    lda_model, vectorizer = train_lda_model(documents, num_topics=N_TOPICS)
    # save model and vectorizer to lda_model.pkl and vectorizer.pkl in topic_model directory
    with open(LDA_PATH, 'wb') as f:
        pkl.dump(lda_model, f)
    with open(VECTORIZER_PATH, 'wb') as f:
        pkl.dump(vectorizer, f)

    print(f"LDA model and vectorizer saved to {LDA_PATH} and {VECTORIZER_PATH}")

    topic_keywords = get_topic_keywords(lda_model, vectorizer)
    with open(KEYWORDS_PATH, 'w') as f:
        for topic_idx, keywords in topic_keywords:
            f.write(f"Topic {topic_idx}: {', '.join(keywords)}\n")

    # SAVE DOCUMENT TOPIC DISTRIBUTIONS
    doc_term_matrix = vectorizer.transform([preprocess_text(doc) for doc in documents])
    doc_topic_distr = lda_model.transform(doc_term_matrix)
    doc_topic_df = pd.DataFrame(doc_topic_distr, columns=[f"Topic_{i}" for i in range(N_TOPICS)])
    doc_topic_df['document'] = documents
    doc_topic_df.to_csv('topic_model/document_topic_distributions.csv', index=False)
    print(f"Document-topic distributions saved to topic_model/document_topic_distributions.csv")
    
    for topic_idx, keywords in topic_keywords:
        print(f"Topic {topic_idx}: {', '.join(keywords)}")


if __name__ == "__main__":
    main()