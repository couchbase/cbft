function SearchCtrl($scope, $http, $routeParams, $log, $sce, $location) {

    $scope.maxPagesToShow = 5;
    $scope.resultsPerPage = 10;
    $scope.page = 1;

    $scope.searchSyntax = function() {
        $scope.numPages = 0;
        $location.search('q', $scope.syntax);
        $location.search('p', $scope.page);
        $scope.results = null;
        from = ($scope.page-1)*$scope.resultsPerPage;
        $http.post('/api/' + $scope.indexName + '/_search', {
            "size": $scope.resultsPerPage,
            "from": from,
            "explain": true,
            "highlight":{},
            "query": {
                "boost": 1.0,
                "query": $scope.syntax,
            },
            "fields": ["*"]
        }).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    if($location.search().p !== undefined) {
        page = parseInt($location.search().p,10);
        if (typeof page == 'number' && !isNaN(page) && isFinite(page) && page > 0 ){
            $scope.page = page;
        }
    }
    if($location.search().q !== undefined) {
        $scope.syntax = $location.search().q;
        $scope.searchSyntax();
    }

    

    $scope.expl = function(explanation) {
            rv = "" + $scope.roundScore(explanation.value) + " - " + explanation.message;
            rv = rv + "<ul>";
            for(var i in explanation.children) {
                    child = explanation.children[i];
                    rv = rv + "<li>" + $scope.expl(child) + "</li>";
            }
            rv = rv + "</ul>";
            return rv;
    };

    $scope.roundScore = function(score) {
            return Math.round(score*1000)/1000;
    };

    $scope.roundTook = function(took) {
        if (took < 1000 * 1000) {
            return "<1ms";
        } else if (took < 1000 * 1000 * 1000) {
            return "" + Math.round(took / (1000*1000)) + "ms";
        } else {
            roundMs = Math.round(took / (1000*1000));
            return "" + roundMs/1000 + "s";
        }
	};

    $scope.setupPager = function(results) {
        $scope.numPages = Math.ceil(results.total_hits/$scope.resultsPerPage);
        $scope.validPages = [];
        for (i = 1; i <= $scope.numPages; i++) {
            $scope.validPages.push(i);
        }


        // now see if we have too many pages
        if ($scope.validPages.length > $scope.maxPagesToShow) {
            numPagesToRemove = $scope.validPages.length - $scope.maxPagesToShow;
            frontPagesToRemove = backPagesToRemove = 0;
            while (numPagesToRemove - frontPagesToRemove - backPagesToRemove > 0) {
                numPagesBefore = $scope.page - 1 - frontPagesToRemove;
                numPagesAfter = $scope.validPages.length - $scope.page - backPagesToRemove;
                if (numPagesAfter > numPagesBefore) {
                    backPagesToRemove++;
                } else {
                    frontPagesToRemove++;
                }
            }

            // remove from the end first, to keep indexes simpler
            $scope.validPages.splice(-backPagesToRemove, backPagesToRemove);
            $scope.validPages.splice(0, frontPagesToRemove);
        }
        $scope.firstResult = (($scope.page-1) * $scope.resultsPerPage) + 1;
    };

    $scope.processResults = function(data) {
        $scope.errorMessage = null;
        $scope.results = data;
        $scope.setupPager($scope.results);
        for(var i in $scope.results.hits) {
                hit = $scope.results.hits[i];
                hit.roundedScore = $scope.roundScore(hit.score);
                hit.explanationString = $scope.expl(hit.explanation);
                hit.explanationStringSafe = $sce.trustAsHtml(hit.explanationString);
                for(var ff in hit.fragments) {
                    fragments = hit.fragments[ff];
                    newFragments = [];
                    for(var ffi in fragments) {
                        fragment = fragments[ffi];
                        safeFragment = $sce.trustAsHtml(fragment);
                        newFragments.push(safeFragment);
                    }
                    hit.fragments[ff] = newFragments;
                }
                if (!hit.fragments) {
                    hit.fragments = {};
                }
                for(var fv in hit.fields) {
                    fieldval = hit.fields[fv];
                    if (hit.fragments[fv] === undefined) {
                        hit.fragments[fv] = [$sce.trustAsHtml(""+fieldval)];
                    }
                }
        }
        $scope.results.roundTook = $scope.roundTook(data.took);
    };

    $scope.jumpToPage = function(pageNum, $event) {
        if ($event) {
            $event.preventDefault();
        }

        $scope.page = pageNum;
        $scope.searchSyntax();
    };

}