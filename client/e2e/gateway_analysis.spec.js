const {expect} = require('chai');

describe('Gateway Analysis', () => {
  afterEach(() => {
    browser.localStorage('DELETE');
  });

  it('should open the analysis diagram section', () => {
    // LOGIN
    browser.url('/');
    browser.waitForEnabled('input[type="text"]');

    browser.setValue('input[type="text"]', 'admin');
    browser.setValue('input[type="password"]', 'admin');
    browser.click('button[type="submit"]');

    browser.waitForExist('.process-definition-card*=Process_1');
    browser.click('.process-definition-card*=Process_1');

    // SELECT VIEW
    browser.waitForExist('.dropdown*=None');
    browser.click('.dropdown*=None');
    browser.click('a*=Branch');

    // SELECT PROCESS DEFINITION
    const endEventSelector = '.djs-element.djs-shape.highlight[data-element-id*="EndEvent"]';
    const gatewaySelector = '.djs-element.djs-shape.highlight[data-element-id*="Gateway"]';

    browser.waitForExist(endEventSelector);

    // CLICK THE END EVENT AND THE GATEWAY
    browser.click(endEventSelector);
    browser.click(gatewaySelector);

    // EXPECT STATISTICS
    browser.waitForVisible('.statisticsContainer');
    expect(browser.isVisible('.statisticsContainer')).to.eql(true);
  });
});
