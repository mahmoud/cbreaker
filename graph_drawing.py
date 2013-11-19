# some handy commands:

import networkx as nx
import matplotlib.pyplot as plt

plt.figure(figsize=(40,30))
nx.draw_spring(comps[0], node_size=20, alpha=0.1, with_labels=False)
plt.savefig('spring_big.png')
plt.clf()
