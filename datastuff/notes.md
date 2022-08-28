## Alternatives

### Single Tensor

The input can be a single intger tensor of dims $m \times 39$, where the first 13 columns are the dense features (which in this case are always integers) and the next 26 are the indexes corresponding to the feature value (token). The target is just a single tensor of $m$ elements. The full batch is a tuple of 2 elements - (input tensor, target tensor)

### Two Tensors

I can divide the input into dense featuers and sparse features and have the first tensor be an integer tensor of dims $m \times 14$ and the second tensor is also an integer tensor of $m \times 26$. The target as usual is a single tensor of $m$ elements. The full batch is then a tuple of 3 elements - (dense features, sparse features, target tensor).

## Discussion

The sparse features in this case are very simple, they only ever have one value and obviously there are no weights. So there is no need to flatten the indexes, capture the offsets, or anything like that. I like the alternative of a single input tensor.

## Storage

S3 infrequent access or standard access makes sense.

Dynamo does not make a lot of sense because the storage costs are too high. The read/write costs are neglible.

DocumentDB is even more expensive than DynamoDB and I have to use EC2 instances + data volumes to host it. If I have data volume, I might as well just read the files serially.

Amazon Keyspaces is Cassandra in the cloud. It is like Dynamo in that there is no need to provision extra EC2 and choose the storage volume, etc. But it is more expensive than Dynamo.

EBS is 4 times more expensive than S3, but its usage is fairly simple.

EFS has this weird Infrequent Access tier which is comparable to S3. But it has a \$0.01/GB charge for IO. So for every epoch of training I'll pay \$10. Which is not good.

I'll start with S3 Infrequent Access tier and see how it goes. If there are problems then the next best thing is S3 standard.

## Compute Meta

Do my processing. This will cost me an EC2 instance running for a few hours. But I'll have to code it up by myself.

EMR will cost me an EC2 instance + EMR pricing for a few hours. But will save me a bunch of coding time.

Athena will cost \$5 per TB. The compute meta might involve multiple scans. So at most it will be \$50. This seems to be the easiest option. Let me try this out on the kaggle criteo dataset and see what happens. The thing is, how do I query multiple files? There is some learning curve here.











