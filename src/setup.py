from setuptools import setup
import setup_translate

pkg = 'Extensions.DvbIManager'
setup(name='enigma2-plugin-extensions-dvbimanager',
      version='0.4.7',
      description='DVB-I channel lists, broadcast fallback, picons and EPG for OpenATV',
      package_dir={pkg: 'DvbIManager'},
      packages=[pkg],
      package_data={pkg: ['images/*.png', '*.png', '*.xml', 'locale/*/LC_MESSAGES/*.mo', 'maintainer.info', 'LICENSE']},
      cmdclass=setup_translate.cmdclass,  # for translation
      )
