FROM python:3.11.4-bookworm

# copy requirements.txt to the docker image
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
CMD [ "python", "./publisher.py" ]
