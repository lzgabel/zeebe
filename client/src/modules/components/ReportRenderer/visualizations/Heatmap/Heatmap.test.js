import React from 'react';
import {mount} from 'enzyme';

import Heatmap from './Heatmap';
import HeatmapOverlay from './HeatmapOverlay';
import {calculateTargetValueHeat} from './service';
import {formatters} from 'services';
import {getRelativeValue} from '../service';

const {convertToMilliseconds} = formatters;

jest.mock('components', () => {
  return {
    BPMNDiagram: props => (
      <div id="diagram">
        Diagram {props.children} {props.xml}
      </div>
    ),
    TargetValueBadge: () => <div>TargetValuesBadge</div>,
    LoadingIndicator: props => (
      <div {...props} className="sk-circle">
        Loading...
      </div>
    )
  };
});
jest.mock('./HeatmapOverlay', () => props => <div>HeatmapOverlay</div>);

jest.mock('./service', () => {
  return {
    calculateTargetValueHeat: jest.fn()
  };
});

jest.mock('../service', () => {
  return {
    getRelativeValue: jest.fn()
  };
});

jest.mock('services', () => {
  const durationFct = jest.fn();
  return {
    formatters: {duration: durationFct, convertToMilliseconds: jest.fn()},
    isDurationReport: jest.fn().mockReturnValue(false)
  };
});

const report = {
  reportType: 'process',
  combined: false,
  data: {
    configuration: {
      xml: 'some diagram XML'
    },
    view: {
      property: 'frequency'
    },
    visualization: 'heat'
  },
  result: {a: 1, b: 2},
  processInstanceCount: 5
};

it('should load the process definition xml', () => {
  const node = mount(<Heatmap report={report} />);

  expect(node).toIncludeText('some diagram XML');
});

it('should load an updated process definition xml', () => {
  const node = mount(<Heatmap report={report} />);

  node.setProps({report: {...report, data: {configuration: {xml: 'another xml'}}}});

  expect(node).toIncludeText('another xml');
});

it('should display a loading indication while loading', () => {
  const node = mount(<Heatmap report={{...report, data: {configuration: {xml: null}}}} />);

  expect(node.find('.sk-circle')).toBePresent();
});

it('should display an error message if visualization is incompatible with data', () => {
  const node = mount(<Heatmap report={{...report, result: 1234}} errorMessage="Error" />);

  expect(node).toIncludeText('Error');
});

it('should display a diagram', () => {
  const node = mount(<Heatmap report={report} />);

  expect(node).toIncludeText('Diagram');
});

it('should display a heatmap overlay', () => {
  const node = mount(<Heatmap report={report} />);

  expect(node).toIncludeText('HeatmapOverlay');
});

it('should convert the data to target value heat when target value mode is active', () => {
  const heatmapTargetValue = {
    active: true,
    values: 'some values'
  };

  mount(<Heatmap report={{...report, data: {configuration: {xml: 'test', heatmapTargetValue}}}} />);

  expect(calculateTargetValueHeat).toHaveBeenCalledWith(report.result, heatmapTargetValue.values);
});

it('should show a tooltip with information about actual and target value', () => {
  const heatmapTargetValue = {
    active: true,
    values: {
      b: {value: 1, unit: 'millis'}
    }
  };

  calculateTargetValueHeat.mockReturnValue({b: 1});
  formatters.duration.mockReturnValueOnce('1ms').mockReturnValueOnce('2ms');
  convertToMilliseconds.mockReturnValue(1);

  const node = mount(
    <Heatmap report={{...report, data: {configuration: {xml: 'test', heatmapTargetValue}}}} />
  );

  const tooltip = node
    .find(HeatmapOverlay)
    .props()
    .formatter('', 'b');

  expect(tooltip.textContent).toContain('target duration: 1ms'.replace(/ /g, '\u00A0'));
  expect(tooltip.textContent).toContain('actual duration: 2ms'.replace(/ /g, '\u00A0'));
  expect(tooltip.textContent).toContain('200% of the target value'.replace(/ /g, '\u00A0'));
});

it('should inform if the actual value is less than 1% of the target value', () => {
  const heatmapTargetValue = {
    active: true,
    values: {
      b: {value: 10000, unit: 'millis'}
    }
  };

  calculateTargetValueHeat.mockReturnValue({b: 10000});
  formatters.duration.mockReturnValueOnce('10000ms').mockReturnValueOnce('1ms');
  convertToMilliseconds.mockReturnValue(10000);

  const node = mount(
    <Heatmap report={{...report, data: {configuration: {xml: 'test', heatmapTargetValue}}}} />
  );

  const tooltip = node
    .find(HeatmapOverlay)
    .props()
    .formatter('', 'b');

  expect(tooltip.textContent).toContain('target duration: 10000ms'.replace(/ /g, '\u00A0'));
  expect(tooltip.textContent).toContain('actual duration: 1ms'.replace(/ /g, '\u00A0'));
  expect(tooltip.textContent).toContain('< 1% of the target value'.replace(/ /g, '\u00A0'));
});

it('should show a tooltip with information if no actual value is available', () => {
  const heatmapTargetValue = {
    active: true,
    values: {
      b: {value: 1, unit: 'millis'}
    }
  };

  calculateTargetValueHeat.mockReturnValue({b: undefined});
  formatters.duration.mockReturnValueOnce('1ms');
  convertToMilliseconds.mockReturnValue(1);

  const node = mount(
    <Heatmap
      report={{...report, result: {}, data: {configuration: {xml: 'test', heatmapTargetValue}}}}
    />
  );

  const tooltip = node
    .find(HeatmapOverlay)
    .props()
    .formatter('', 'b');

  expect(tooltip.textContent).toContain('target duration: 1ms'.replace(/ /g, '\u00A0'));
  expect(tooltip.textContent).toContain('No actual value available.'.replace(/ /g, '\u00A0'));
  expect(tooltip.textContent).toContain(
    'Cannot compare target and actual value'.replace(/ /g, '\u00A0')
  );
});

it('should show the relative frequency in a tooltip', () => {
  getRelativeValue.mockClear();
  getRelativeValue.mockReturnValue('12.3%');

  const node = mount(<Heatmap report={report} formatter={v => v} />);

  const tooltip = node
    .find(HeatmapOverlay)
    .props()
    .formatter(3);

  expect(getRelativeValue).toHaveBeenCalledWith(3, 5);
  expect(tooltip).toBe('3\u00A0(12.3%)');
});

it('should not display an error message if data is valid', () => {
  const node = mount(<Heatmap report={report} errorMessage="Error" />);

  expect(node).not.toIncludeText('Error');
});
