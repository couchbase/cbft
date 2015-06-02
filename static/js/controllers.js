'use strict';

/* Controllers */

angular.module('myApp.controllers', []);

function errorMessage(errorMessageFull, code) {
    console.log("errorMessageFull", errorMessageFull, code);
    var a = (errorMessageFull || (code + "")).split("err: ");
    return a[a.length - 1];
}
