module.exports = {
  plugins: {
    '@stylexjs/postcss-plugin': {
      include: ['src/**/*.{js,jsx,ts,tsx}'],
      babelConfig: {
        babelrc: false,
        parserOpts: { plugins: ['typescript', 'jsx'] },
        plugins: [
          [
            '@stylexjs/babel-plugin',
            {
              dev: process.env.NODE_ENV !== 'production',
              runtimeInjection: false,
              enableInlinedConditionalMerge: true,
              treeshakeCompensation: true,
              aliases: {
                '@/*': ['/ROOT/src/*'],
              },
              unstable_moduleResolution: {
                type: 'commonJS',
                rootDir: '.',
              },
            },
          ],
        ],
      },
      useCSSLayers: true,
    },
    autoprefixer: {},
  },
};
