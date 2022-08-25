## Alternatives

### Single Tensor

The input can be a single intger tensor of dims $m \times 39$, where the first 13 columns are the dense features (which in this case are always integers) and the next 26 are the indexes corresponding to the feature value (token). The target is just a single tensor of $m$ elements. The full batch is a tuple of 2 elements - (input tensor, target tensor)

### Two Tensors

I can divide the input into dense featuers and sparse features and have the first tensor be an integer tensor of dims $m \times 14$ and the second tensor is also an integer tensor of $m \times 26$. The target as usual is a single tensor of $m$ elements. The full batch is then a tuple of 3 elements - (dense features, sparse features, target tensor).

## Discussion

The sparse features in this case are very simple, they only ever have one value and obviously there are no weights. So there is no need to flatten the indexes, capture the offsets, or anything like that. I like the alternative of a single input tensor.













