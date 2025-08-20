/**
* @NApiVersion 2.1
* @NScriptType Restlet
* @NModuleScope Public
*/

/* 
FIXED GL Export RESTlet v3 for Modern Animal
This version uses the ORIGINAL WORKING period filter method from savedSearch_20250729.js
Deploy this as a NEW script with NEW IDs
*/

var 
    log,
    search,
    record,
    response = new Object();     

define(['N/log', 'N/search', 'N/record'], main);

function main(logModule, searchModule, recordModule) {
    log = logModule;
    search = searchModule;
    record = recordModule;
    
    return { post: postProcess }
}

function postProcess(request) {     
    try {
        log.debug('Fixed GL Export v3', 'Request received: ' + JSON.stringify(request));
        
        // Handle different request types
        if (request.action === 'getAccounts') {
            return getAccountList(request);
        } else if (request.action === 'getSubsidiaries') {
            return getSubsidiaryList(request);
        } else if (request.action === 'getPeriods') {
            return getPeriodList(request);
        } else {
            // Default: run GL search with filters
            return runGLSearchFixed(request);
        }
        
    } catch(e) {     
        log.error('Fixed GL Export v3 Error', e);
        return { 
            'success': false,
            'error': { 
                'type': e.type || 'UnknownError', 
                'name': e.name || 'Error', 
                'message': e.message || 'Unknown error occurred' 
            } 
        }
    }     
}

function getAccountList(request) {
    log.debug('getAccountList', 'Getting account list');
    
    try {
        var accountSearch = search.create({
            type: search.Type.ACCOUNT,
            columns: [
                search.createColumn({name: 'internalid'}),
                search.createColumn({name: 'number'}),
                search.createColumn({name: 'name'}),
                search.createColumn({name: 'type'})
            ],
            filters: [
                search.createFilter({
                    name: 'isinactive',
                    operator: search.Operator.IS,
                    values: false
                })
            ]
        });
        
        var accounts = [];
        var resultSet = accountSearch.run();
        var results = [];
        var start = 0;
        
        do {
            results = resultSet.getRange({ start: start, end: start + 1000 });
            start += 1000;
            
            results.forEach(function(result) {
                accounts.push({
                    id: result.getValue('internalid'),
                    number: result.getValue('number'),
                    name: result.getValue('name'),
                    type: result.getText('type')
                });
            });
            
        } while (results.length);
        
        log.debug('getAccountList', 'Found ' + accounts.length + ' accounts');
        
        return {
            'success': true,
            'action': 'getAccounts',
            'results': accounts,
            'count': accounts.length
        };
        
    } catch(e) {
        throw e;
    }
}

function getSubsidiaryList(request) {
    log.debug('getSubsidiaryList', 'Getting subsidiary list');
    
    try {
        var subsidiarySearch = search.create({
            type: search.Type.SUBSIDIARY,
            columns: [
                search.createColumn({name: 'internalid'}),
                search.createColumn({name: 'name'})
            ],
            filters: [
                search.createFilter({
                    name: 'isinactive',
                    operator: search.Operator.IS,
                    values: false
                })
            ]
        });
        
        var subsidiaries = [];
        var resultSet = subsidiarySearch.run();
        var results = [];
        var start = 0;
        
        do {
            results = resultSet.getRange({ start: start, end: start + 1000 });
            start += 1000;
            
            results.forEach(function(result) {
                subsidiaries.push({
                    id: result.getValue('internalid'),
                    name: result.getValue('name')
                });
            });
            
        } while (results.length);
        
        return {
            'success': true,
            'action': 'getSubsidiaries', 
            'results': subsidiaries,
            'count': subsidiaries.length
        };
        
    } catch(e) {
        throw e;
    }
}

function getPeriodList(request) {
    log.debug('getPeriodList', 'Getting accounting period list');
    
    try {
        var periodSearch = search.create({
            type: search.Type.ACCOUNTING_PERIOD,
            columns: [
                search.createColumn({name: 'internalid'}),
                search.createColumn({name: 'periodname'}),
                search.createColumn({name: 'startdate'}),
                search.createColumn({name: 'enddate'}),
                search.createColumn({name: 'closed'})
            ],
            filters: []
        });
        
        var periods = [];
        var resultSet = periodSearch.run();
        var results = [];
        var start = 0;
        
        do {
            results = resultSet.getRange({ start: start, end: start + 1000 });
            start += 1000;
            
            results.forEach(function(result) {
                periods.push({
                    id: result.getValue('internalid'),
                    name: result.getValue('periodname'),
                    startDate: result.getValue('startdate'),
                    endDate: result.getValue('enddate'),
                    closed: result.getValue('closed')
                });
            });
            
        } while (results.length);
        
        return {
            'success': true,
            'action': 'getPeriods',
            'results': periods,
            'count': periods.length
        };
        
    } catch(e) {
        throw e;
    }
}

function runGLSearchFixed(request) {
    log.debug('runGLSearchFixed v3', 'Running FIXED GL search using ORIGINAL WORKING method');
    
    if (!request.searchID) {          
        throw { 
            'type': 'error.SavedSearchAPIError', 
            'name': 'INVALID_REQUEST', 
            'message': 'No searchID was specified.' 
        }               
    }     

    var searchObj = search.load({ id: request.searchID });
    
    // LOG ORIGINAL FILTERS FOR DEBUGGING
    var originalFilters = searchObj.filters || [];
    log.debug('Original Filters', 'Saved search has ' + originalFilters.length + ' filters');
    originalFilters.forEach(function(filter, index) {
        log.debug('Original Filter ' + index, 'Name: ' + filter.name + ', Join: ' + (filter.join || 'none') + ', Operator: ' + filter.operator);
    });
    
    // HANDLE PERIOD FILTER USING ORIGINAL WORKING METHOD
    if (request.periodId || request.periodName) {
        log.debug('Adding Period Filter v3', {
            periodId: request.periodId,
            periodName: request.periodName
        });
        
        // Remove any existing period filters (EXACTLY like original)
        searchObj.filters = searchObj.filters.filter(function(filter) {
            var isPostingPeriodFilter = (filter.name === 'postingperiod');
            var isPeriodNameFilter = (filter.name === 'periodname' && filter.join === 'accountingperiod');
            
            if (isPostingPeriodFilter || isPeriodNameFilter) {
                log.debug('Filter Removed v3', 'Removed existing period filter: ' + filter.name + (filter.join ? ' (join: ' + filter.join + ')' : ''));
                return false; // Remove this filter
            }
            return true; // Keep this filter
        });
        
        // Add new period filter (EXACTLY like original)
        if (request.periodId) {
            // Filter by period internal ID
            searchObj.filters.push(search.createFilter({
                name: 'postingperiod',
                operator: search.Operator.ANYOF,
                values: request.periodId
            }));
            log.debug('NEW Filter Applied v3', 'Period ID: ' + request.periodId);
        } else if (request.periodName) {
            // Filter by period name (ORIGINAL WORKING METHOD)
            searchObj.filters.push(search.createFilter({
                name: 'periodname',
                join: 'accountingperiod',
                operator: search.Operator.IS,
                values: request.periodName
            }));
            log.debug('NEW Filter Applied v3', 'Period Name: ' + request.periodName + ' (using periodname + accountingperiod join)');
        }
    }
    
    // Handle date range filters if provided (EXACTLY like original)
    if (request.dateFrom || request.dateTo) {
        log.debug('Adding Date Filter v3', {
            from: request.dateFrom,
            to: request.dateTo
        });
        
        searchObj.filters.push(search.createFilter({
            name: 'trandate',
            operator: search.Operator.WITHIN,
            values: [request.dateFrom || '1/1/2000', request.dateTo || '12/31/2099']
        }));
    }
    
    // Account filtering for chunking
    if (request.accountIds && request.accountIds.length > 0) {
        searchObj.filters.push(search.createFilter({
            name: 'account',
            operator: search.Operator.ANYOF,
            values: request.accountIds
        }));
        log.debug('NEW Filter Applied v3', 'Account IDs: ' + request.accountIds.join(','));
    } else if (request.accountNumbers && request.accountNumbers.length > 0) {
        var accountIds = getAccountIdsByNumbers(request.accountNumbers);
        if (accountIds.length > 0) {
            searchObj.filters.push(search.createFilter({
                name: 'account',
                operator: search.Operator.ANYOF,
                values: accountIds
            }));
            log.debug('NEW Filter Applied v3', 'Account Numbers converted to IDs: ' + accountIds.join(','));
        }
    }
    
    // Subsidiary filtering
    if (request.subsidiaryIds && request.subsidiaryIds.length > 0) {
        searchObj.filters.push(search.createFilter({
            name: 'subsidiary',
            operator: search.Operator.ANYOF,
            values: request.subsidiaryIds
        }));
        log.debug('NEW Filter Applied v3', 'Subsidiary IDs: ' + request.subsidiaryIds.join(','));
    }
    
    // LOG FINAL FILTERS FOR DEBUGGING
    log.debug('Final Filters v3', 'Search now has ' + searchObj.filters.length + ' filters');
    searchObj.filters.forEach(function(filter, index) {
        log.debug('Final Filter ' + index, 'Name: ' + filter.name + ', Join: ' + (filter.join || 'none') + ', Operator: ' + filter.operator);
    });
    
    // Execute search with pagination and safety limits
    var results = [];
    var resultSet = searchObj.run();
    var start = parseInt(request.startIndex) || 0;
    var maxResults = parseInt(request.maxResults) || 10000;
    var pageSize = Math.min(1000, maxResults);
    
    log.debug('Search Execution v3', 'Start: ' + start + ', Max: ' + maxResults + ', Page Size: ' + pageSize);
    
    var totalRetrieved = 0;
    var searchResults = [];
    var iterations = 0;
    var maxIterations = 50; // Safety limit
    
    do {
        iterations++;
        if (iterations > maxIterations) {
            log.error('Search Safety v3', 'Breaking pagination after ' + maxIterations + ' iterations');
            break;
        }
        
        var endIndex = Math.min(start + pageSize, start + (maxResults - totalRetrieved));
        log.debug('Search Range v3', 'Iteration ' + iterations + ': range ' + start + ' to ' + endIndex);
        
        searchResults = resultSet.getRange({ start: start, end: endIndex });
        
        if (searchResults.length > 0) {
            results = results.concat(searchResults);
            totalRetrieved += searchResults.length;
            start += searchResults.length;
            
            log.debug('Search Progress v3', 'Retrieved: ' + totalRetrieved + ' of max ' + maxResults);
        } else {
            log.debug('Search End v3', 'No more results at iteration ' + iterations);
        }
        
        if (totalRetrieved >= maxResults || searchResults.length === 0) {
            break;
        }
        
    } while (searchResults.length > 0 && totalRetrieved < maxResults && iterations < maxIterations);
    
    var hasMoreResults = searchResults.length === pageSize && totalRetrieved < maxResults;
    
    log.debug('Search Complete v3', 'Final count: ' + results.length + ', Has more: ' + hasMoreResults);
    
    return {
        'success': true,
        'results': results,
        'count': results.length,
        'startIndex': parseInt(request.startIndex) || 0,
        'hasMoreResults': hasMoreResults,
        'nextStartIndex': hasMoreResults ? start : null,
        'metadata': {
            'version': 'v3',
            'periodFilter': request.periodId || request.periodName || 'none',
            'executionTime': new Date().toISOString(),
            'originalFilterCount': originalFilters.length,
            'finalFilterCount': searchObj.filters.length
        }
    };
}

function getAccountIdsByNumbers(accountNumbers) {
    try {
        var accountSearch = search.create({
            type: search.Type.ACCOUNT,
            columns: [search.createColumn({name: 'internalid'})],
            filters: [
                search.createFilter({
                    name: 'number',
                    operator: search.Operator.ANYOF,
                    values: accountNumbers
                })
            ]
        });
        
        var accountIds = [];
        var results = accountSearch.run().getRange({ start: 0, end: 1000 });
        
        results.forEach(function(result) {
            accountIds.push(result.getValue('internalid'));
        });
        
        return accountIds;
        
    } catch(e) {
        log.error('getAccountIdsByNumbers v3', 'Error converting account numbers: ' + e.message);
        return [];
    }
}