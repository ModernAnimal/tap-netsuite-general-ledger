/**
* @NApiVersion 2.1
* @NScriptType Restlet
* @NModuleScope Public
*/

/* This Macro is saved and run in NetSuite. Saving copy here for posterity. */

define(['N/log', 'N/search', 'N/runtime'], function(log, search, runtime) {
    
    function postProcess(request) {
        try {
            log.debug('RESTlet Request', request);
            
            // Validate request
            if (!request.searchID) {
                throw {
                    type: 'error.SavedSearchAPIError',
                    name: 'INVALID_REQUEST',
                    message: 'No searchID was specified.'
                };
            }
            
            // Load the saved search
            var searchObj = search.load({
                id: request.searchID
            });
            
            // Handle period filter if provided
            if (request.periodId || request.periodName) {
                log.debug('Adding Period Filter', {
                    periodId: request.periodId,
                    periodName: request.periodName
                });
                
                // Remove any existing period filters
                searchObj.filters = searchObj.filters.filter(function(filter) {
                    return filter.name !== 'postingperiod';
                });
                
                // Add new period filter
                if (request.periodId) {
                    // Filter by period internal ID
                    searchObj.filters.push(search.createFilter({
                        name: 'postingperiod',
                        operator: search.Operator.ANYOF,
                        values: request.periodId
                    }));
                } else if (request.periodName) {
                    // Filter by period name
                    searchObj.filters.push(search.createFilter({
                        name: 'periodname',
                        join: 'accountingperiod',
                        operator: search.Operator.IS,
                        values: request.periodName
                    }));
                }
            }
            
            // Handle date range filters if provided
            if (request.dateFrom || request.dateTo) {
                log.debug('Adding Date Filter', {
                    from: request.dateFrom,
                    to: request.dateTo
                });
                
                searchObj.filters.push(search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.WITHIN,
                    values: [request.dateFrom || '1/1/2000', request.dateTo || '12/31/2099']
                }));
            }
            
            // Execute search
            var results = [];
            var pagedData = searchObj.runPaged({
                pageSize: 1000
            });
            
            log.debug('Search Count', pagedData.count);
            
            // Check governance
            var remainingUsage = runtime.getCurrentScript().getRemainingUsage();
            log.debug('Governance Units Available', remainingUsage);
            
            // Process all pages
            pagedData.pageRanges.forEach(function(pageRange) {
                // Check governance
                if (runtime.getCurrentScript().getRemainingUsage() < 100) {
                    log.error('Governance Warning', 'Low governance units');
                    return false;
                }
                
                var page = pagedData.fetch({index: pageRange.index});
                page.data.forEach(function(result) {
                    results.push(result);
                    return true;
                });
            });
            
            log.debug('Results Retrieved', results.length);
            
            return {
                success: true,
                results: results,
                metadata: {
                    count: results.length,
                    periodFilter: request.periodId || request.periodName || 'none',
                    executionTime: new Date().toISOString()
                }
            };
            
        } catch (e) {
            log.error('RESTlet Error', e);
            return {
                success: false,
                error: {
                    type: e.type || e.name || 'error',
                    message: e.message || 'An error occurred',
                    details: e.toString()
                }
            };
        }
    }
    
    return {
        post: postProcess
    };
});