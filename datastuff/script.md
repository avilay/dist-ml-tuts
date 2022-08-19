# Data Stuff

This set of tutorials are about the awesome data utilities of PyTorch. PyTorch bundles a lot of common datasets for computer vision, language, audio, etc. {Show the different webpages listing these datasets.} While these are very useful when learning Machine Learning or doing novel research to beat established benchmarks, they are not of much use for practical applications. Like building a recommendation system for your ecommerce business, analysing patient and disease records for the Health Maintenance Orgnaization you work for, ==TODO: IoT example==, etc. This is because your training data does not come neatly packaged in mathematical tensors ready to be used by your models. Your ecommerce transactions could be living in a distributed PostGreSQL cluster, your IoT sensor telemetry could be coming in via Kafka streams, patient records are probably stored in data lakes backed by Hive or S3. This tutorial series will show how to use PyTorch data libraries to convert these kinds of real world datasets into mathematical tensors that can be fed to your cool neural net models.

## Dataset and DataLoader

There are two main classes that we will learn about - `Dataset` and `DataLoader`. And of these `Dataset` is actually an interface (a Python abstract class). A dataset object represents your entire data, whether that data stored on your hard drive and fits in your computer's memory, or whether it is stored in a data lake over multiple servers. There are two types of datasets, map style datasets and iterable style datasets. Both of them serve a single purpose, and that is to retrieve a single instance of data from the underlying datastore. And this single instance can be a SQL row, a CSV row, a PIL image along with its label, and audio file along with its text, etc. But when we feed training data to our neural net, we first need to sample these instances into a batch and then collate these rows into a mathematical tensor. This is what the `DataLoader` class does. This is a heavyweight class that can do a lot and we will dig into the details in later videos.

## Map Style Datasets and DataLoader

In this video we will understand what are map style datasets and how to use them for training. Remember the main purpose of a dataset is to return a single instance of the data. For map style datasets this is done via indexing. So if this is my ecommerce order history dataset then `orders[0]` will return one instance and `orders[1]` will return another instance and so on. As implementors of this dataset we need to ensure that every time we call `orders[0]` it will return the same instance as before. This is needed when we are sampling from this dataset. Another requirement from this style of dataset is that we should be able to get its length. `len(orders)` should return the total number of instances in our dataset. Here is what the source code of `Dataset` looks like. While the interface is simple enough, our implementation can be as complex as we need it to be. For example,  if we want to build our ecommerce training set from orders that were sent as gifts, our SQL query might do a three way join between the user table, shipping table, and order table and then filter for where the user's home address is different from the order's shipping address.

Lets build a toy dataset that we can play with as we learn how to use the `DataLoader` object. We can see that it satisfies both our criteria.

> Implement a dataset that returns a dict or a SQL cursor or something like that, not a numpy array.









## Archive

We will start with learning how to use a couple of bundled datasets - ==TODO: CV dataset==, ==TODO: NLP dataset==. This will help us as we next dig into architecture of the underlying libraries used by these datasets. As we get a deeper and better understanding of these libraries, we will work towards our two "capstone projects" if you will - training with data in a SQL database, and training with data in an S3 data lake.

