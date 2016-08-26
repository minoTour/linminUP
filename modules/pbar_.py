from progressbar import *
from pbar import *

n = len(ks)
bar = mkBar(n)
bar.start()
bar.update(i)
bar.finish()
