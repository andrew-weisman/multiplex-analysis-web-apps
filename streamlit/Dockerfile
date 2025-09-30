# Use Miniconda as base image
# In the future, pin this version to ensure consistency.
# FROM continuumio/miniconda3:latest
FROM mambaorg/micromamba:latest

# Set the working directory in the container
WORKDIR /app

# Copy ONLY the environment file first (changes less frequently)
COPY source/environment.yml .

# Create conda environment from environment.yml
# RUN conda env create -f environment.yml
RUN micromamba install -y -f environment.yml && micromamba clean --all --yes

# Copy source files to the container
COPY source/ .

# Copy .git to get commit hash
COPY .git .git

# Expose the port that Streamlit runs on
EXPOSE 8501

# Run the Streamlit app when the container starts
CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
