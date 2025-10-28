# Add the MongoDB Homebrew tap. In the Terminal app, run the following command:
brew tap mongodb/brew

# Install the MongoDB Community Edition:

brew install mongodb-community

# This installs the latest version of MongoDB. To install an older version, specify the version number, for example:

brew install mongodb-community@6.0

# Run MongoDB. To start MongoDB as a service:


brew services start mongodb/brew/mongodb-community

# Check that the MongoDB service is running:

brew services list