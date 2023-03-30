# AARS - Aleph Active Record SDK
AARS is a powerful and flexible Python library built on top of the Aleph decentralized storage network, designed to help you build better backends for your decentralized applications. It provides an easy-to-use interface for managing and querying your data, with a focus on performance and versatility.

## Features
- Asynchronous, high-performance data storage and retrieval
- Customizable schema with support for different data types
- Indexing for efficient querying
- Revision history tracking for records
- Support for forgetting data (GDPR compliant)
- Built-in pagination for large result sets

## Installation
Install AARS using pip:

```shell
pip install aars
```

## Getting Started
To get started with AARS, you will need to define your data schema by creating classes that inherit from Record. These classes represent the objects you want to store and query on the Aleph network.

Here's an example of how you can implement a simple social media platform, that we'll call "Chirper":

```python
from src.aars import Record, Index, AARS
from typing import List


class User(Record):
    username: str
    display_name: str
    bio: Optional[str]

class Chirp(Record):
    author: User
    content: str
    likes: int
    timestamp: int
```
In this example, we have a User class representing a user of Chirper, and a Chirp class representing a user's message. Now, let's create some indices to make querying our data more efficient:

```python
Index(User, 'username')
Index(Chirp, 'author')
Index(Chirp, 'timestamp')
```
With the schema defined and indices created, we can now perform various operations, such as creating new records, querying records, and updating records:

```python
# Create a new user
new_user = await User(username='chirpy_user', display_name='Chirpy User', bio='I love chirping!').save()

# Create a new chirp
new_chirp = await Chirp(author=new_user, content='Hello, Chirper!', likes=0, timestamp=int(time.time())).save()

# Query chirps by author
chirps_by_author = await Chirp.where_eq(author=new_user).all()

# Update a chirp
new_chirp.likes += 1
updated_chirp = await new_chirp.save()
```

## Documentation
For detailed documentation, including advanced features such as revision history, forgetting data, and pagination,
refer to the docs folder in the repository or [visit the official documentation website](https://aleph-im.github.io/active-record-sdk/).

## Building the Docs
To build the documentation, you will need to install the dependencies listed in the requirements.txt and docs-requirements.txt. Then, run the following command:

```shell
mkdocs build
```

You can serve the documentation locally by running the following command:

```shell
mkdocs serve
```

## Contributing
Contributions to AARS are welcome! If you have found a bug, want to suggest an improvement, or have a question, feel free to open an issue on the GitHub repository.

## License
AARS is released under the [MIT License](https://github.com/aleph-im/active-record-sdk/blob/main/LICENSE).
