from parsl import python_app
import os
from hashlib import md5

@python_app
def wordcount_bucketed(input_file, out_dir, B, idx):
    os.makedirs(out_dir, exist_ok=True)

    outs = [open(f"{out_dir}/map_{idx}_bucket_{b}.txt", "w") for b in range(B)]

    with open(input_file, "r") as f:
        for line in f:
            for w in line.split():
                h = int(md5(w.encode()).hexdigest(), 16) % B
                outs[h].write(f"{w} 1\n")

    for o in outs:
        o.close()
        
    return True

@python_app
def reduce_bucket(f1, f2, out_path, _dep1=None, _dep2=None):
    counts = {}

    with open(f1, "r") as a:
        for line in a:
            k, v = line.split()
            counts[k] = counts.get(k, 0) + int(v)

    with open(f2, "r") as b:
        for line in b:
            k, v = line.split()
            counts[k] = counts.get(k, 0) + int(v)

    # grava resultado
    with open(out_path, "w") as o:
        for k, v in counts.items():
            o.write(f"{k} {v}\n")

    return out_path
