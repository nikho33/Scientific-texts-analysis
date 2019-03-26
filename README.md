# Scientific text analysis  

The project aims to analyse scientific texts through machine learning and big data tools
  
Using pyspark framework (https://spark.apache.org/docs/2.2.1/api/python/pyspark.html), we would like to cluster under classes a batch of scientific texts or papers. After clustering the texts by similarities, we would like also to name the classes according to the samples inside the class. 

## Methods
*... here comes some details on the methods to implement*

Check LDA (Latent Dirichlet Allocation) method:  
In natural language processing, latent Dirichlet allocation (LDA) is a generative statistical model that allows sets of observations to be explained by unobserved groups that explain why some parts of the data are similar. For example, if observations are words collected into documents, it posits that each document is a mixture of a small number of topics and that each word's presence is attributable to one of the document's topics. LDA is an example of a topic model. (https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)


## Data sources
### General
https://www.istex.fr/  
https://arxiv.org/

### Biotech and microfluidic related papers
Abstracts and keywords from Lab-on-Chip and Nature journals
https://pubs.rsc.org/en/journals/journalissues/lc?_ga=2.96347164.1406498630.1553624405-1609253475.1553624405#!recentarticles&adv  
https://www.nature.com/
