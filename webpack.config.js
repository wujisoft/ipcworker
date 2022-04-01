const path = require('path');
const TerserPlugin = require('terser-webpack-plugin');
const DtsBundlePlugin = require('dts-bundle-webpack');

module.exports = {
    mode: 'production',
    entry: './src/index.ts',
    externals: {
        'yargs': 'yargs',
        'async_hooks': 'async_hooks',
        'redis': 'redis'
    },
    module: {
        rules: [{
            test: /\.ts$/,
            use: [{ 
                loader: 'ts-loader',
                options: {
                    configFile: "tsconfig.webpack.json"
                }
            }],
            exclude: /node_modules|test/
        }]
    },
    resolve: {
        extensions: ['.ts', '.js'],
    },
    output: {
        filename: 'ipcworker.js',
        path: path.resolve(__dirname, 'dist'),
        library: {
            type: 'umd',
            name: 'IPCWorker'
        },
        globalObject: 'this'
    },
    optimization: {
        minimize: true,
        minimizer: [new TerserPlugin({
            terserOptions: {
                compress: true,
                module: true
            },
            extractComments: {
                filename: (fd) => `${fd.filename}.txt`,
                banner: () => ''
            }
        })],
    },
    plugins: [
        new DtsBundlePlugin({
            name: 'ipcworker',
            main: 'dist/index.d.ts',
            out: 'ipcworker.d.ts',
            removeSource: true,
        })
    ]
}