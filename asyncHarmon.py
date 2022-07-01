import time
from harmonic.api import HarmonicClient
import concurrent.futures

# This module wraps around the core harmonicAI API
# and increases performance by leveraging python
# threading. While asyncio can be used,
# it's interface and API are still not really
# finalized and change from version to version,
# so sticking to threading for right now

# Harmonic client wrapper with async support
class AsyncHarmonicWrapper:
    # Takes in a pre-constructed harmonic client from your codebase as the only
    # constructor input
    def __init__(self, harmonicClient : HarmonicClient, numWorkers=10) -> None:
        self.numWorkers = numWorkers
        self.rawClient = harmonicClient
        self.pool = concurrent.futures.ThreadPoolExecutor(numWorkers)

    # A general function to parallelize IO callables. Takes in [callables, [(params for each callable)]]
    # and ends up returning a list of results, with ordering preserved.
    def _parralelizeCallables(self, listOfCallables, listOfParamTuples):
        # Queing up all our requests
        futureObjs = []
        for i in range(len(listOfCallables)):
            futureObj = self.pool.submit(listOfCallables[i], *(listOfParamTuples[i]))
            futureObjs.append(futureObj)
        
        # Waiting for all our future objects to resolve. Repeatedly
        # polls and checks if all objects are resolved. If any object
        # fails, loop has to repeat
        fullyResolved = False
        while not fullyResolved:
            for future in futureObjs:
                if not future.done():
                    # Sleeping to stop this loop from pushing CPU to 100% util
                    time.sleep(0.2)
                    break
            # If none of the futures fail, fullyResolved = true
            fullyResolved = True
        
        # Resolving and returning results
        results = [futureObj.result() for futureObj in futureObjs]
        return results

    # Runs a bunch of enrich company calls in parralel. Takes in a list of
    # url or enrichmnent requests
    def enrich_companies(self, url_or_enrichment_requests: list): 
        # Converting our list of urls into a list of argument tuples
        argumentTupleList = [(urlArg,) for urlArg in url_or_enrichment_requests]

        # Creating our callable list. Basically just repeatedly add the core enrich
        # company callable to the callable list, making sure to match the number of args
        # and callables
        callableList = []
        for i in range(len(url_or_enrichment_requests)):
            callableList.append(self.rawClient.enrich_company)

        # Execute them and return results
        return self._parralelizeCallables(callableList, argumentTupleList)

    # Runs a single encrich company call just to make this wrapper complete
    def enrich_company(self, url_or_enrichment_request):
        return self.rawClient.enrich_company(url_or_enrichment_request)
    
    # Runs a batch of encrich people calls in parralel. Takes in a list of urls
    def enrich_people(self, listOfURLS: list):
        # Converting our URLs into a list of argument tuples
        arugmentTupleList = [(url,) for url in listOfURLS]

        # Creating a list of n callables, where n is the length of the url list; just
        # making one callable per function call
        callableList = []
        for i in range(len(listOfURLS)):
            callableList.append(self.rawClient.enrich_person)
        
        # Execute and return results
        return self._parralelizeCallables(callableList, arugmentTupleList)
    
    # Runs a single encrich people call just to make this API complete
    def enrich_person(self, url):
        return self.rawClient.enrich_person(url)
    
    # Gets all saved searches
    def get_saved_searches(self):
        return self.rawClient.get_saved_searches()
    
    # Gets saved searches by owner
    def get_saved_searches_by_owner(self):
        return self.rawClient.get_saved_searches_by_owner()

    # This function is just here to complete the API, it doesn't
    # actually run in parralel: I don't see why that would be useful
    def get_saved_search_results(self, saved_search_id, record_processor=None, page_size=100):
        return self.rawClient.get_saved_search_results(saved_search_id, record_processor, page_size)
    
    # Runs a bunch of search queries in parralel and returns their results. Takes a list of param tuples;
    # each tuple takes the exact same params as the search function in this wrapper, but is packed into a tuple.
    # Simply pass in a list of those tuples
    def batch_searches(self, listOfArgTuples):
        # Making a list of n callables to run
        callableList = []
        for i in range(len(listOfArgTuples)):
            callableList.append(self.rawClient.search)
    
        # Running and returning results
        return self._parralelizeCallables(callableList, listOfArgTuples)
    
    # Search function to complete the API
    def search(self, keywords_or_query, page=0, page_size=50, include_results=True):
        return self.rawClient.search(keywords_or_query, page, page_size, include_results)
    
    # There's a quirk in the API which is that there seems to be an inconsistency
    # between Harmonic's Search and Lookup datastores, since not all URNs returned
    # by searches can be found when looking them up. This function "safely" looks up
    # URNs; if they aren't found, it simply returns null
    def safe_get_company_by_urn(self, urn):
        try:
            return self.rawClient.get_company_by_id(urn)
        except:
            return None
    
    # Batches a lot of safe company URN lookups together.
    def batch_safe_get_company_by_urn(self, listOfURNs : list):
        paramTupleList = [ (urn,) for urn in listOfURNs ]
        # Create a list of callables, with one callable per URN; basically,
        # just make sure there are enough callables for all the URNs, one callable
        # per set of paramaters
        callableList = [self.safe_get_company_by_urn for i in range(len(listOfURNs))]

        return self._parralelizeCallables(callableList, paramTupleList)

    def get_company_by_id(self, id):
        return self.rawClient.get_company_by_id(id)

    def get_companies_by_ids(self, ids, isURN=False):
        return self.rawClient.get_companies_by_ids(ids, isURN)

    def get_person_by_id(self, id):
        return self.rawClient.get_person_by_id(id)
    
    def get_persons_by_ids(self, ids, isURN=False):
        return self.rawClient.get_persons_by_ids(ids, isURN)

    def set_watchlist(
        self,
        watchlist_id,
        name=None,
        companies=None,
        shared_with_team=None,
    ):
        return self.rawClient.set_watchlist(watchlist_id, name, companies, shared_with_team)
    
    def delete_watchlist(self, watchlist_id):
        return self.rawClient.delete_watchlist(watchlist_id)
    
    def get_watchlists(self):
        return self.rawClient.get_watchlists()
    
    def get_watchlist_by_id(self, watchlist_id):
        return self.rawClient.get_watchlist_by_id(watchlist_id)
    
    def add_company_to_watchlist(self, watchlist_id, company_ids, isURN=False):
        return self.rawClient.add_company_to_watchlist(watchlist_id, company_ids, isURN)

    def add_company_to_watchlist_by_urls(self, watchlist_id, company_urls):
        return self.rawClient.add_company_to_watchlist_by_urls(watchlist_id, company_urls)
    
    def remove_company_from_watchlist(self, watchlist_id, company_ids, isURN=False):
        return self.rawClient.remove_company_from_watchlist(watchlist_id, company_ids, isURN)