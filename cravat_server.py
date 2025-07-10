from flask import Flask, request, jsonify
from inference_util import analyze_coding_effect

app = Flask(__name__)

@app.route('/analyze', methods=['GET'])
def analyze():
    try:
        # Get parameters from query string
        chr = request.args.get('chr')
        pos = request.args.get('pos')
        ref = request.args.get('ref')
        alt = request.args.get('alt')

        # Validate required parameters
        if not all([chr, pos, ref, alt]):
            return jsonify({
                'error': 'Missing required parameters. Need chr, pos, ref, alt'
            }), 400

        results = analyze_coding_effect(chr, pos, ref, alt)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5060)

