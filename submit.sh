#!/bin/bash
#SBATCH --nodes 10
#SBATCH --ntasks-per-node=48
#SBATCH -p sequana_cpu_bigmem
#SBATCH --job-name=WordCount
#SBATCH --time=00:50:00
#SBATCH -e slurm-%j.err
#SBATCH -o slurm-%j.out

module load anaconda3/2024.02_sequana
eval "$(conda shell.bash hook)"

CONDA_ENV="/scratch/cenapadrjsd/reiglan.lourenco/RNA-seq/conda_envs/parsl_env/"
conda activate ${CONDA_ENV}

CDIR="/scratch/cenapadrjsd/reiglan.lourenco/WordCount_Parsl/"

INPUT="${CDIR}/inputs48GB/*.txt"
OUTPUT="${CDIR}/result100GB.txt"

mkdir -p "${CDIR}/outputs"

python main.py -i "$INPUT" -o "$OUTPUT" --onslurm
