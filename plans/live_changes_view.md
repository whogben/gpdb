# Live Changes View

Add a checkbox to the View Graph that enables Live view, where changes to the Graph that happen on the backend result in updates to the current view graph.

The method for doing this is up to you, ideally we'll focus on what's changed - but until we have huge graphs we can just load the entire matching graph to the client each time it changes.

My goal is that i should see new nodes/edges appearing in the live dynamic visualization, without it blowing away my current viewing like the pan and arrangement of nodes etc. But if that's seriously hard, we can skip that to start.

As far as tech, I'm open to a MVP that periodicaly polls like every 1 second or so and only receives data if there's changes to what the filter would include.

However, optimally, if you don't think it's too hard, lets do something even smarter with even better latency.

What we don't want is something too complex and unmaintanable. At a minimum, a way to not have to hit refresh to see graph changes while, for example, your AI is making graph changes.

It might be wise to extract this live graph widget into something that can potentially be used beyond the graph screen at this time so it's nicely encapsulated. But that's not a strict requirement at this stage.