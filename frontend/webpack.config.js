const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');

module.exports = {
  entry: './src/index.ts',
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: 'ts-loader',
        exclude: /node_modules/
      }
    ]
  },
  resolve: {
    extensions: ['.ts', '.js']
  },
  output: {
    filename: 'bundle.js',
    path: path.resolve(__dirname, 'dist'),
    clean: true // optional: clean dist before build
  },
  plugins: [
    new HtmlWebpackPlugin({
      template: './src/index.html',
      filename: 'index.html'
    })
  ],
  mode: 'development',
  devServer: {
    static: './dist',
    open: true
  },
  module: {
    rules: [
      {
        test: /\.css$/i,  // Match .css files
        use: ['style-loader', 'css-loader'],
      },
      {
        test: /\.tsx?$/,  // If using TypeScript
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
resolve: {
    extensions: ['.ts', '.tsx', '.js'],
  },
devServer: {
    static: './dist', // Or wherever your HTML is served from
  },
};