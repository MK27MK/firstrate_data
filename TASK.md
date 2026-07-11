The task is to complete the implementation of this project, `firstrate_data` making it fully functional.

Comprehensively fetch the official API docs at and return to them when needed.

I started scaffolding the implementation at @loader.py . I want you to complete it, without forgetting to:

- uv init the project
- full, unambiguous type hinting on arguments and return types
- comprehensive method documentation by pasting verbatim the descriptions provided by the firstrate API reference (like in the `download_historical_data` method I already scaffolded.)
- implementing the ability to save the data to self._directory, whose structure is kept coherent and clean by FirstRateLoader

Right now you will only focus on the method that I already scaffolded, forgetting the ones like Ticker Listing, Last Update, etc.