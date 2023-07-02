Data Preprocessing: 

1. Preprocessing done on dataset ( we used 'Arts.txt.gz' here as an example) in 'main.py' file ('python3 main.py' to run the file)
2. Data has been preprocessed in parsed chunks using one product category at one time and finally, all the data is merged and uploaded onto mongodb.

Data Visualization:

1. Load the data from https://drive.google.com/file/d/1PPVtquVOKHVfm4rEaM-T-Hia6Pn8PXTJ/view?usp=sharing google drive and upload to mongodb (make sure database name is 'amazon_reviews' and collection has to be 'reviews) 
2. Data_Visualization.ipynb is used for data visualization.

Sentiment Analysis and clustering:

1. text_clustering_nlp.ipynb jupyter notebook performs the following:
    - initiates a spark session and connects with mongodb instance
    - tokenization, lemmatization, context_spell_check, n-gram generation, and vectorization using BERT word embeddings
    - perform sentiment analysis on reviews and categorize into positive and negative
    - perform clustering of negative reviews 
    - sample data visualization of number of product-related and service-related reviews after clustering of 'Arts' data
    - evaluates system based on execution time as the no of cores are increased
2. Run text_clustering_nlp.ipynb to get clustering results and program evaluation results
3. Load the data from https://drive.google.com/file/d/14-d6XxiX4Wo56opjXrBOgE6auQN396lg/view?usp=sharing and upload to mongodb (make sure the database name is '532Proj' and collection name is 'arts')
