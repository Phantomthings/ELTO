<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;

class DashboardController extends Controller
{
    /**
     * Display the dashboard with error charge analysis.
     */
    public function index(Request $request)
    {
        // Date range for the entire period (to be fetched from database in production)
        $dateMin = '2024-01-01';
        $dateMax = date('Y-m-d');

        return view('dashboard', [
            'date_min' => $dateMin,
            'date_max' => $dateMax,
        ]);
    }
}
