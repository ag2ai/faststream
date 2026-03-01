---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Lifespan Events

Sometimes you need to define the logic that should be executed before launching the application.
This means that the code will be executed once - even before your application starts receiving messages.

Also, you may need to terminate some processes after stopping the application. In this case, your code will also be executed exactly once:
but after the completion of the main application.

Since this code is executed before the application starts and after it stops, it covers the entire lifecycle *(lifespan)* of the application.

This can be very useful for initializing your application settings at startup, raising a pool of connections to a database, or running machine learning models.
