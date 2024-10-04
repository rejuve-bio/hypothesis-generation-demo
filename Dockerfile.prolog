# Use an official SWI-Prolog image
FROM swipl:latest

# Set the working directory for Prolog files
WORKDIR /app/pl

# Copy Prolog files from the local directory
COPY ./pl /app/pl

# Install SWI-Prolog dependencies
RUN apt-get update && apt-get install -y swi-prolog

# Expose port 4243
EXPOSE 4243

# Command to load the necessary Prolog files and start the server
ENTRYPOINT swipl -s load_kbs.pl -s meta_interpreter.pl -s queries.pl -s rules.pl -g server_start
