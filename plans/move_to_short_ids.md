# Short IDs

Our current massive UUID4s are taking up too much AI context - they need to pass both a graph_id and typically at least a node_id and often multiple other ids at the same time. UUID4 is overkill for this - in fact it makes it actively difficult to use.

What I'd like to do is look at the function we're using to generate ids, and turn it into something shorter - some kind of id slug, that is still sufficiently unique to have very low collision probability, and perhaps when we generate IDs we can do so in a loop ensuring that there *isnt* a collision.

The new IDs should be easy for humans and AI to read, with a maximum of 7 random characters, alphanumeric but all lowercase, and optionally hyphens to assist with readability. The goal here is that we'll have easy to read/write ids that can't be mistaken for each other.

I want to keep the ability for users of the gpdb utility to replace that choice with whatever ID generator they want, e.g. they can switch to uuid4 by replacing a function or something like that in downstream projects, but by default, we should be generating much nicer slug-like ids.