# get python 11
FROM python:3.11

# install preliminary requirements
RUN apt-get update -y\
    && apt-get install -y git\
    && apt-get install -y libudunits2-dev\
    && apt-get install -y libgdal-dev\
    && apt-get install -y awscli\
    && apt-get install -y libssl-dev\
    && apt-get install -y libffi-dev\
    && apt-get install -y build-essential

RUN python3 -m pip install cryptography

# copy working directory
COPY . /root/pipeline-gsheets

# use the bohemia kenya work directory
WORKDIR /root/pipeline-gsheets

# create virtual environment
RUN python3 -m venv aws_src/venv
RUN . aws_src/venv/bin/activate
RUN pip install -r aws_src/requirements.txt

# create virtual environment
RUN python3 -m venv gsheets_src/venv
RUN . gsheets_src/venv/bin/activate
RUN python3 -m pip install pygsheets
RUN python3 -m pip install pandas

# run makefile
CMD make pipeline