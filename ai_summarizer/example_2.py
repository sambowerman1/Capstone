from person_summarizer import Person

albert_einstein = Person("https://en.wikipedia.org/wiki/Albert_Einstein")
summary = albert_einstein.summarize_sync()  # Use the synchronous version
print(summary)