# Streamliner Known Issues
 
## "This site can’t be reached" error while trying to download streamliner
This error is triggered due to Streamliner repository migration, switch to [Streamliner Changelog](https://docs.customer.phdata.io/docs/streamliner/latest/changelog/) and find the updated download url just below the respective Streamliner version.
 
## "Could not resolve host: repoitory.phdata.io" error while running streamliner-fetch script
This error is triggered due to Streamliner repository migration and the `streamliner-fetch` script is still referring to Streamliner's old repository. To resolve this issue, update the Streamliner `bin/streamliner-fetch` script with the updated download url. You can find the updated download url just below the respective Streamliner version in the [Streamliner Changelog](https://docs.customer.phdata.io/docs/streamliner/latest/changelog/).