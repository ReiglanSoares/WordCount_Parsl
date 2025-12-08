import glob
import argparse
import os
import shutil
import parsl

from config import gen_config
from apps import wordcount_bucketed, reduce_bucket

def tree_reduce_bucket(bucket_files, bucket_id):
    tasks = bucket_files.copy()
    n = 1
    L = len(tasks)

    while n < L:
        for r in range(0, L, 2*n):
            if r + n < L:
                out_file = f"intermediate/reduce_bucket_{bucket_id}_{r}_{r+n}.txt"
                tasks[r] = reduce_bucket(tasks[r], tasks[r+n], out_file)
        n *= 2

    return tasks[0]

def run(files, output_file, B):

    os.makedirs("outputs", exist_ok=True)
    os.makedirs("intermediate", exist_ok=True)

    bucket_lists = [[] for _ in range(B)]

    for idx, f in enumerate(files):
        future_list = wordcount_bucketed(f, "intermediate", B, idx)

        for b in range(B):
            bucket_lists[b].append(future_list[b])

    final_bucket_files = []

    for b in range(B):
        print(f"[INFO] Reduzindo bucket {b}/{B-1} com {len(bucket_lists[b])} arquivos...")
        final_future = tree_reduce_bucket(bucket_lists[b], b)
        final_bucket_files.append(final_future)
      final_paths = [f.result() for f in final_bucket_files]

    out_final = os.path.join("outputs", output_file)

    with open(out_final, "w") as fout:
        for fpath in final_paths:
            with open(fpath, "r") as f:
                for line in f:
                    fout.write(line)

    print("\n[OK] Resultado final salvo em:", out_final)
    print("[INFO] Limpando arquivos intermediários...")
    shutil.rmtree("intermediate")

    print("[OK] Intermediários removidos.")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("--buckets", type=int, default=256,
                        help="Número de buckets para particionar palavras")
    parser.add_argument("--onslurm", action="store_true")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--monitor", action="store_true")
    args = parser.parse_args()

    cfg = gen_config(
        threads=args.threads,
        monitoring=args.monitor,
        slurm=args.onslurm
    )
    parsl.load(cfg)

    files = sorted(glob.glob(args.input))
    print(f"[INFO] {len(files)} arquivos detectados.")

    run(files, args.output, args.buckets)
