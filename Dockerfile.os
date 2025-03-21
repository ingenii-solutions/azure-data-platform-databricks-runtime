FROM databricksruntime/standard:15.4-LTS

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PYTHONUNBUFFERED 1

RUN apt update && apt upgrade -y && apt clean
RUN apt install -y libpython3.11-dev

# Install rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"