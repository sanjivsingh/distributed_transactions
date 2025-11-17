import random
import string
import time
from Word_dictionary_without_DB.indexed_dictionary import IndexedWordDictionaryRefresh

# Function to generate a random word
def generate_random_word(length=7):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Function to generate a random meaning (simple sentence)
def generate_random_meaning():
    words = ['a', 'the', 'is', 'of', 'to', 'and', 'in', 'that', 'it', 'with', 'for', 'on', 'by', 'at']
    return ' '.join(random.choices(words, k=random.randint(5, 100)))


def trigger_jenkins_job():
    import requests
    # Replace with your Jenkins job URL.
    # Currently triggering App Server to refresh dictionary
    jenkins_url = "http://localhost:8000/refresh/dictionary"
    try:
        response = requests.get(jenkins_url)
        if response.status_code == 200:
            print("Jenkins job triggered successfully.")
        else:
            print(f"Failed to trigger Jenkins job. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error triggering Jenkins job: {e}")

def update_dictionary_worker(manifest_file: str, num_new_words: int = 100):

    while True:
        """Load existing dictionary, add new words, and save."""
        # Add existing words if any (placeholder; in practice, load from file)
        # For demo, I am generating new random words.
        # In real scenario, you would load existing words from a source and add new ones.
        # Add new random words
        added_count = 0
        existing_words = {}
        for _ in range(num_new_words):
            word = generate_random_word()
            meaning = generate_random_meaning()
            if word not in existing_words:
                existing_words[word] = meaning
                added_count += 1
        # For testing, add a known word
        existing_words["sanjiv"] = f"A proper noun representing a name. sanjiv singh {time.time()}"
        
        writer = IndexedWordDictionaryRefresh(manifest_file)
        # Save the updated dictionary
        writer.update_dictionary(existing_words)
        print(f"Added {added_count} new words")
        trigger_jenkins_job()
        time.sleep(60)  # Sleep for a minute before next update

if __name__ == "__main__":
    manifest_file = "Word_dictionary_without_DB/data/manifest.txt"
    update_dictionary_worker(manifest_file, num_new_words=10)





