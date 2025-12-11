<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="csrf-token" content="{{ csrf_token() }}">
    <title>Connexion - ELTO Dashboard</title>
    <style>
        :root {
            --color-bg: #f8fafc;
            --color-surface: #ffffff;
            --color-border: #e2e8f0;
            --color-text: #1e293b;
            --color-muted: #64748b;
            --color-primary: #dc2626;
            --radius: 10px;
        }

        * { box-sizing: border-box; }

        body {
            margin: 0;
            font-family: 'DM Sans', Arial, sans-serif;
            background: var(--color-bg);
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            color: var(--color-text);
        }

        .card {
            background: var(--color-surface);
            border: 1px solid var(--color-border);
            border-radius: var(--radius);
            padding: 2rem;
            width: 100%;
            max-width: 420px;
            box-shadow: 0 12px 30px rgba(0,0,0,0.06);
        }

        .logos {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 1.5rem;
            margin-bottom: 1.5rem;
        }

        .logo {
            height: 40px;
            width: auto;
            object-fit: contain;
        }

        h1 { margin-bottom: 0.25rem; font-size: 1.5rem; }
        p { margin-top: 0; color: var(--color-muted); margin-bottom: 1.5rem; }

        label { display: block; margin-bottom: 0.25rem; font-weight: 600; }
        input[type="email"], input[type="password"] {
            width: 100%;
            padding: 0.75rem 0.9rem;
            border-radius: 8px;
            border: 1px solid var(--color-border);
            background: #f9fafb;
            font-size: 1rem;
            margin-bottom: 1rem;
        }

        button {
            width: 100%;
            padding: 0.85rem;
            border: none;
            border-radius: 8px;
            background: var(--color-primary);
            color: white;
            font-size: 1rem;
            font-weight: 700;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
        }

        button:hover { opacity: 0.95; }
        button:disabled { opacity: 0.7; cursor: not-allowed; }

        .spinner {
            width: 16px;
            height: 16px;
            border: 2px solid rgba(255, 255, 255, 0.3);
            border-top-color: white;
            border-radius: 50%;
            animation: spin 0.6s linear infinite;
            display: none;
        }

        button.loading .spinner { display: block; }
        button.loading .btn-text { display: none; }

        @keyframes spin {
            to { transform: rotate(360deg); }
        }

        .alert { margin-top: 1rem; padding: 0.75rem; border-radius: 8px; font-size: 0.95rem; }
        .alert.error { background: #fee2e2; color: #b91c1c; border: 1px solid #fecaca; }
        .alert.success { background: #ecfdf3; color: #166534; border: 1px solid #bbf7d0; }

        .remember-row {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }
        .remember-row input[type="checkbox"] {
            width: auto;
            margin: 0;
        }
    </style>
</head>
<body>
    <div class="card">
        <div class="logos">
            <img src="{{ asset('assets/elto.png') }}" alt="ELTO Logo" class="logo">
            <img src="{{ asset('assets/nidec.png') }}" alt="Nidec Logo" class="logo">
        </div>

        <h1>Connexion</h1>
        <p>Identifiez-vous pour acceder au dashboard.</p>

        <form method="POST" action="{{ route('login') }}" id="login-form">
            @csrf
            <label for="email">Email</label>
            <input type="email" id="email" name="email" placeholder="utilisateur@exemple.com" value="{{ old('email') }}" required autofocus>

            <label for="password">Mot de passe</label>
            <input type="password" id="password" name="password" placeholder="********" required>

            <div class="remember-row">
                <input type="checkbox" id="remember" name="remember">
                <label for="remember" style="margin-bottom: 0;">Se souvenir de moi</label>
            </div>

            <button type="submit" id="submit-btn">
                <span class="spinner"></span>
                <span class="btn-text">Se connecter</span>
            </button>
        </form>

        @if ($errors->any())
        <div class="alert error">
            @foreach ($errors->all() as $error)
                {{ $error }}
            @endforeach
        </div>
        @endif
    </div>

    <script>
        const form = document.getElementById('login-form');
        const submitBtn = document.getElementById('submit-btn');

        form.addEventListener('submit', () => {
            submitBtn.classList.add('loading');
            submitBtn.disabled = true;
        });
    </script>
</body>
</html>
